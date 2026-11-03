#!/usr/bin/env bash
# E5 핫 파티션 (#64, 07 스펙)
#
# 메시지 키를 정한 뒤 한 종목 쏠림 부하(TOP1_RATIO)를 걸고, E3처럼 운영 큐 두 개를
# DETACH → STOP_SEC 대기 → ATTACH 해서 쌓인 backlog를 파티션별로 따라잡는 과정을 1초 간격으로 기록한다.
#
# 사용법: bash experiments/scripts/e5-run.sh <account|symbol> <run>
#   전제: producer-tool 빌드 (cd experiments/producer-tool && ./gradlew installDist)
#   환경변수: RATE(80) TOP1_RATIO(0.5) WARMUP_SEC(60) STOP_SEC(300) RESOLVE_LAG(10000)
#             PART_RESOLVE_LAG(2000) MAX_CATCHUP_SEC(900) OPEN_DRAIN_SEC(600) PRE_DRAIN_SEC(1800)
#
# 기록 (<결과 폴더>/raw/rate<RATE>-top<TOP1_RATIO>/<키>-r<회차>/)
#   boundary-to-<키>-*.txt : 키 전환 직전 끝 오프셋 (e5-switch-key.sh)
#   offsets-detach.txt     : DETACH 직전 끝 오프셋
#   offsets-attach.txt     : ATTACH 직전 끝 오프셋 (두 파일 차이 = 중단 구간 파티션별 유입량)
#   lag.csv                : 1초 간격 그룹 lag 합계
#   lag-partitions.csv     : 1초 간격 파티션별 lag
#   members.csv            : 파티션별 담당 컨슈머 (바뀔 때만)
#   attach.txt             : 큐별 ATTACH 직전 시각(ms). 해소 시간은 큐마다 자기 ATTACH 시각 기준
#   partition-summary.csv  : 파티션별 담당 컨슈머, 유입량, ATTACH 직전 backlog, 해소 시간
#   events.log, summary.txt, reconcile.txt, k6.log
#
# - 부하는 E3와 같은 이유로 80 TPS를 기본으로 한다 (체결 유입은 시뮬레이터 속도로 정해진다).
# - 계좌 키로 되돌리는 일은 이 스크립트가 하지 않는다. 종목 키 실행 뒤 e5-switch-key.sh account로 되돌린다.
set -euo pipefail

export MSYS_NO_PATHCONV=1

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <account|symbol> <run>" >&2
  exit 1
fi
KEY="$1"; RUN="$2"
case "$KEY" in
  account|symbol) ;;
  *) echo "키는 account 또는 symbol이어야 함: $KEY" >&2; exit 1 ;;
esac

RATE="${RATE:-80}"
TOP1_RATIO="${TOP1_RATIO:-0.5}"
WARMUP_SEC="${WARMUP_SEC:-60}"
STOP_SEC="${STOP_SEC:-300}"
RESOLVE_LAG="${RESOLVE_LAG:-10000}"            # 토픽 합계: 묶음 1개(kafka_max_block_size) 이하면 해소
PART_RESOLVE_LAG="${PART_RESOLVE_LAG:-2000}"   # 파티션 하나: 이 값 이하면 해소
MAX_CATCHUP_SEC="${MAX_CATCHUP_SEC:-900}"
OPEN_DRAIN_SEC="${OPEN_DRAIN_SEC:-600}"
PRE_DRAIN_SEC="${PRE_DRAIN_SEC:-1800}"
RESULT_DIR="${RESULT_DIR:-experiments/results/20261104-E5-hot-partition}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TAG="${KEY}-r${RUN}"
OUT="$ROOT/$RESULT_DIR/raw/rate${RATE}-top${TOP1_RATIO}/${TAG}"
K6_NAME="e5-k6-${TAG}"
LAG_NAME="e5-lag-${TAG}"
TOOL="$ROOT/experiments/producer-tool/build/install/producer-tool"
[ -x "$TOOL/bin/producer-tool" ] || { echo "producer-tool 빌드 필요: (cd experiments/producer-tool && ./gradlew installDist)" >&2; exit 1; }

if [ -e "$OUT" ]; then echo "이미 결과가 있음: $OUT" >&2; exit 1; fi
mkdir -p "$OUT"

# shellcheck disable=SC2016  # 비밀번호 변수는 컨테이너 안에서 풀려야 한다
ch() { docker exec mts-clickhouse sh -c 'clickhouse-client --user "${CLICKHOUSE_USER:-default}" --password "$CLICKHOUSE_PASSWORD" -q "$1"' sh "$1"; }
# shellcheck disable=SC2016
mysql_q() { docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'; }
now_ms() { date +%s%3N; }
event() { echo "$(now_ms) $*" | tee -a "$OUT/events.log"; }

# 최신 그룹 lag 합계 (lag.csv 마지막 줄). 못 읽은 값은 -1
lags() {
  tail -n 1 "$OUT/lag.csv" 2>/dev/null | awk -F, '$1 ~ /^[0-9]+$/ {printf "%d,%d", $2, $3; ok = 1} END {if (!ok) printf "-1,-1"}'
}
open_orders() { mysql_q <<< "SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL');"; }

# --- 사전 확인 -------------------------------------------------------------
docker compose -f "$ROOT/docker-compose.yml" ps order-server --format '{{.Health}}' | grep -q healthy \
  || { echo "order-server가 healthy가 아님" >&2; rm -rf "$OUT"; exit 1; }
ch "SELECT 1" > /dev/null || { echo "ClickHouse 접속 실패" >&2; rm -rf "$OUT"; exit 1; }

# 같은 출발 상태: 열린 주문 0
deadline=$(( $(date +%s) + PRE_DRAIN_SEC ))
while :; do
  open="$(open_orders)"
  [ "$open" = "0" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { echo "시작 전 열린 주문이 ${open}건 남음" >&2; rm -rf "$OUT"; exit 1; }
  sleep 10
done

# --- 키 전환 -----------------------------------------------------------------
bash "$ROOT/experiments/scripts/e5-switch-key.sh" "$KEY" "$OUT" | tee -a "$OUT/events.log"

# --- lag 샘플러 (그룹 합계, 파티션별, 배정) ------------------------------------
NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker run -d --name "$LAG_NAME" --network "$NET" -v "$TOOL:/app:ro" -v "$OUT:/out" \
  eclipse-temurin:21.0.12.1_1-jre-noble /app/bin/producer-tool lag \
  --groups clickhouse-executions:mts.trading.executions,clickhouse-orders:mts.trading.orders \
  --interval-ms 1000 --out /out/lag.csv \
  --partitions-out /out/lag-partitions.csv --members-out /out/members.csv >/dev/null
for _ in $(seq 30); do [ "$(lags)" != "-1,-1" ] && break; sleep 1; done
[ "$(lags)" != "-1,-1" ] || { echo "lag 샘플러가 값을 쓰지 않음: docker logs $LAG_NAME" >&2; exit 1; }
event "start tag=$TAG rate=$RATE top1=$TOP1_RATIO warmup=${WARMUP_SEC}s stop=${STOP_SEC}s lags=$(lags)"

cleanup() {
  docker rm -f "$K6_NAME" "$LAG_NAME" >/dev/null 2>&1 || true
  # 중간에 끊겨도 큐가 떼어진 채 남지 않게
  for q in kafka_executions_queue kafka_orders_queue; do
    [ "$(ch "EXISTS TABLE mts.$q")" = "1" ] || ch "ATTACH TABLE mts.$q"
  done
}
trap cleanup EXIT

# --- 부하 (중단, 따라잡기 내내 유지) --------------------------------------------
LOAD_SEC=$(( WARMUP_SEC + STOP_SEC + MAX_CATCHUP_SEC + 120 ))
docker run -d --name "$K6_NAME" --network "$NET" -v "$ROOT/load-test:/scripts" grafana/k6:2.3.0 run --quiet \
  -e RATE="$RATE" -e TOP1_RATIO="$TOP1_RATIO" -e DURATION="${LOAD_SEC}s" /scripts/orders.js >/dev/null
event "load started"
sleep "$WARMUP_SEC"

# --- 중단 ------------------------------------------------------------------
bash "$ROOT/experiments/scripts/e5-offsets.sh" "$OUT/offsets-detach.txt" > /dev/null
ch "DETACH TABLE mts.kafka_executions_queue"
ch "DETACH TABLE mts.kafka_orders_queue"
event "detached"
sleep "$STOP_SEC"

# --- 재개, 해소까지 대기 -------------------------------------------------------
bash "$ROOT/experiments/scripts/e5-offsets.sh" "$OUT/offsets-attach.txt" > /dev/null
# 두 큐는 순서대로 붙으므로 먼저 붙은 체결 큐가 1~2초 먼저 소비를 시작한다.
# 해소 시간과 backlog는 큐마다 자기 ATTACH 직전 시각을 기준으로 계산한다.
ATTACH_EXEC_MS="$(now_ms)"; ch "ATTACH TABLE mts.kafka_executions_queue"
ATTACH_ORD_MS="$(now_ms)";  ch "ATTACH TABLE mts.kafka_orders_queue"
printf 'clickhouse-executions,%s\nclickhouse-orders,%s\n' "$ATTACH_EXEC_MS" "$ATTACH_ORD_MS" > "$OUT/attach.txt"
event "attached exec=$ATTACH_EXEC_MS orders=$ATTACH_ORD_MS"

resolved=""
deadline=$(( $(date +%s) + MAX_CATCHUP_SEC ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  IFS=, read -r e o <<< "$(lags)"
  if [ "$e" -ge 0 ] && [ "$o" -ge 0 ] && [ "$e" -le "$RESOLVE_LAG" ] && [ "$o" -le "$RESOLVE_LAG" ]; then resolved=1; break; fi
  sleep 1
done
if [ -n "$resolved" ]; then event "resolved exec_lag=$e orders_lag=$o"; else event "timeout exec_lag=$e orders_lag=$o"; fi

# 해소 후 30초 정상 상태를 남기고 부하 종료
sleep 30
docker stop "$K6_NAME" >/dev/null; event "load stopped"
docker logs "$K6_NAME" > "$OUT/k6.log" 2>&1 || true

# 정합성은 움직임이 멈춘 뒤에 본다
deadline=$(( $(date +%s) + OPEN_DRAIN_SEC ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  open="$(open_orders)"
  [ "$open" = "0" ] && break; sleep 5
done
event "open orders=$open"
for _ in $(seq 120); do
  IFS=, read -r e o <<< "$(lags)"
  [ "$e" = "0" ] && [ "$o" = "0" ] && break; sleep 1
done
event "drained exec_lag=$e orders_lag=$o"
docker rm -f "$LAG_NAME" >/dev/null 2>&1 || true

# --- 정합성 (건수 대조) ------------------------------------------------------
my="$(mysql_q <<< "SELECT (SELECT COUNT(*) FROM orders), (SELECT COUNT(*) FROM executions);" | tr '\t' ' ')"
chc="$(ch "SELECT (SELECT count() FROM mts.orders FINAL), (SELECT count() FROM mts.executions FINAL)" | tr '\t' ' ')"
{
  echo "MySQL      (orders executions): $my"
  echo "ClickHouse (orders executions): $chc"
  if [ "$my" = "$chc" ]; then echo "결과: 일치"; else echo "결과: 불일치"; fi
} > "$OUT/reconcile.txt"

# --- 파티션별 요약 ------------------------------------------------------------
# 그룹, 파티션, 담당 컨슈머(클라이언트 번호), 중단 구간 유입량, ATTACH 직전 backlog, 해소 시간(초)
# backlog는 그 큐의 ATTACH 직전 마지막 lag 샘플이다 (ATTACH 뒤 첫 샘플은 이미 소비가 시작된 값이다).
awk -F'[:,]' -v thr="$PART_RESOLVE_LAG" '
  BEGIN {
    topic["clickhouse-executions"] = "mts.trading.executions"
    topic["clickhouse-orders"] = "mts.trading.orders"
  }
  FILENAME ~ /\/attach\.txt$/ { at[$1] = $2; next }   # offsets-attach.txt와 구분
  FILENAME ~ /offsets-detach/ { d[$1 "," $2] = $3; next }
  FILENAME ~ /offsets-attach/ { inflow[$1 "," $2] = $3 - d[$1 "," $2]; next }
  FILENAME ~ /members/ {
    if (FNR > 1 && $1 >= at[$2] && $5 != "-" && !(($2 "," $3) in c)) { n = split($5, z, "-"); c[$2 "," $3] = "c" z[n] }
    next
  }
  FILENAME ~ /lag-partitions/ {
    if (FNR == 1) next
    k = $2 "," $3
    if (!(k in seen)) { seen[k] = 1; order[++cnt] = k }
    if ($1 < at[$2]) { b[k] = $6; next }
    if (!(k in r) && $6 >= 0 && $6 <= thr) r[k] = sprintf("%.1f", ($1 - at[$2]) / 1000)
    next
  }
  END {
    print "group,partition,consumer,inflow_stop,backlog_before_attach,resolve_sec"
    for (i = 1; i <= cnt; i++) {
      k = order[i]; split(k, a, ",")
      print k "," ((k in c) ? c[k] : "-") "," inflow[topic[a[1]] "," a[2]] "," b[k] "," ((k in r) ? r[k] : "-")
    }
  }' "$OUT/attach.txt" "$OUT/offsets-detach.txt" "$OUT/offsets-attach.txt" "$OUT/members.csv" "$OUT/lag-partitions.csv" \
  > "$OUT/partition-summary.csv"

# --- 요약 ------------------------------------------------------------------
{
  echo "tag=$TAG rate=$RATE top1=$TOP1_RATIO stop=${STOP_SEC}s part_resolve_lag=$PART_RESOLVE_LAG"
  awk -F, -v ae="$ATTACH_EXEC_MS" -v ao="$ATTACH_ORD_MS" -v thr="$RESOLVE_LAG" '
    NR == 1 { next }
    {
      if (re == "" && $1 >= ae && $2 >= 0 && $2 <= thr) re = sprintf("%.1f", ($1 - ae) / 1000)
      if (ro == "" && $1 >= ao && $3 >= 0 && $3 <= thr) ro = sprintf("%.1f", ($1 - ao) / 1000)
    }
    END { printf "토픽 해소 (lag 합계 <= %d, 체결/주문): %s / %s초\n", thr, (re == "" ? "-" : re), (ro == "" ? "-" : ro) }
  ' "$OUT/lag.csv"
  awk -F, '
    NR == 1 { next }
    {
      g = $1; n[g]++; si[g] += $4; sb[g] += $5
      if ($4 > mi[g]) mi[g] = $4
      if ($5 > mb[g]) mb[g] = $5
      if ($6 == "-") un[g]++; else if ($6 + 0 > mr[g]) mr[g] = $6 + 0
      k = g "," $3; cb[k] += $5
      if ($6 != "-" && $6 + 0 > cr[k]) cr[k] = $6 + 0
    }
    END {
      for (g in n) {
        printf "[%s] 유입 최대/평균 %.2f, backlog 최대/평균 %.2f, 가장 늦은 파티션 해소 %.1f초, 미해소 파티션 %d\n",
          g, (si[g] > 0 ? mi[g] / (si[g] / n[g]) : 0), (sb[g] > 0 ? mb[g] / (sb[g] / n[g]) : 0), mr[g], un[g] + 0
      }
      for (k in cb) {
        split(k, a, ",")
        printf "[%s] 컨슈머 %s backlog %d, 해소 %.1f초, 처리량 %.0f건/s\n",
          a[1], a[2], cb[k], cr[k], (cr[k] > 0 ? cb[k] / cr[k] : 0)
      }
    }' "$OUT/partition-summary.csv" | sort
  cat "$OUT/reconcile.txt"
} | tee "$OUT/summary.txt"