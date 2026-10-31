#!/usr/bin/env bash
# E3 컨슈머 중단 후 따라잡기 (#43)
#
# 주문 부하(기본 80 TPS)를 건 상태에서 운영 큐 두 개를 DETACH → STOP분 대기 → ATTACH 하고,
# 부하를 계속 넣으면서 lag가 해소될 때까지 1초 간격으로 기록한다. 끝나면 부하를 멈추고 정합성을 대조한다.
#
# 사용법: bash experiments/scripts/e3-run.sh <tpc:0|1> <stop_min> <run>
#   전제: e3-set-tpc.sh <tpc> 로 큐 설정을 맞춘 상태, 두 컨슈머 그룹 lag가 작을 것
#         producer-tool 빌드 (cd experiments/producer-tool && ./gradlew installDist) — lag 샘플러로 사용
#   환경변수: RATE(80) PRE_DRAIN_SEC(1800) WARMUP_SEC(60) STOP_SEC(stop_min*60, 시험 실행용) RESOLVE_LAG(10000) MAX_CATCHUP_SEC(900)
#
# 기록 (raw/<tag>/)
#   lag.csv     : 1초 간격 lag 원본 (producer-tool lag, Kafka Admin API로 끝 오프셋 - 커밋 오프셋)
#   samples.csv : 1초 간격 — 시각(ms), 단계, 체결 lag, 주문 lag, ClickHouse MemoryTracking(바이트)
#   events.log  : 단계 전환 시각
#   summary.txt : 첫 소비 대기, 해소 시간, lag·메모리 최고치
#   k6.log, reconcile-*.txt
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <tpc:0|1> <stop_min> <run>" >&2
  exit 1
fi
TPC="$1"; STOP_MIN="$2"; RUN="$3"
# 시뮬레이터는 초당 약 87건의 주문만 체결 처리한다(체결 유입 약 365/s ÷ 주문당 체결 4.2건).
# 300 TPS에서는 열린 주문이 매초 200건 넘게 쌓여 회차마다 출발 상태가 달라졌다(#43 1차 측정).
# 체결 유입은 시뮬레이터 속도로 정해지므로, 부하를 시뮬레이터가 감당하는 80으로 낮춰도 쌓이는 체결량은 거의 같다.
RATE="${RATE:-80}"
WARMUP_SEC="${WARMUP_SEC:-60}"
STOP_SEC="${STOP_SEC:-$(( STOP_MIN * 60 ))}"
RESOLVE_LAG="${RESOLVE_LAG:-10000}"     # 묶음 1개(kafka_max_block_size) 이하면 해소로 본다
MAX_CATCHUP_SEC="${MAX_CATCHUP_SEC:-900}"
OPEN_DRAIN_SEC="${OPEN_DRAIN_SEC:-600}"   # 부하 종료 후 열린 주문이 0이 될 때까지 최대 대기 (주문 대조는 열린 주문 0에서만 정확)
PRE_DRAIN_SEC="${PRE_DRAIN_SEC:-1800}"    # 시작 전 열린 주문이 0이 될 때까지 최대 대기 (앞 회차 상태가 넘어오지 않게)

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TAG="tpc${TPC}-stop${STOP_MIN}m-r${RUN}"
OUT="$ROOT/experiments/results/20261028-E3-consumer-catchup/raw/${TAG}"
K6_NAME="e3-k6-${TAG}"
LAG_NAME="e3-lag-${TAG}"
TOOL="$ROOT/experiments/producer-tool/build/install/producer-tool"
[ -x "$TOOL/bin/producer-tool" ] || { echo "producer-tool 빌드 필요: (cd experiments/producer-tool && ./gradlew installDist)" >&2; exit 1; }

if [ -e "$OUT" ]; then echo "이미 결과가 있음: $OUT" >&2; exit 1; fi
mkdir -p "$OUT"

ch() { docker exec mts-clickhouse clickhouse-client -q "$1"; }
now_ms() { date +%s%3N; }
event() { echo "$(now_ms) $*" | tee -a "$OUT/events.log"; }

mysql_q() { docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'; }

# 최신 lag (lag.csv 마지막 줄). 아직 없거나 못 읽은 값은 -1 → 0으로 오인해 해소 판정하지 않게
# kafka-exporter는 부하 중 그룹 지표를 빠뜨리고(시험 실행에서 -1 다수), JVM 도구는 호출마다 2~3초라 쓰지 않는다
lags() {
  tail -n 1 "$OUT/lag.csv" 2>/dev/null | awk -F, '$1 ~ /^[0-9]+$/ {printf "%d,%d", $2, $3; ok = 1} END {if (!ok) printf "-1,-1"}'
}
mem() { ch "SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'"; }

# --- 사전 확인 -------------------------------------------------------------
actual_tpc="$(ch "SELECT extract(engine_full, 'kafka_thread_per_consumer = ([0-9]+)') FROM system.tables
                  WHERE database = 'mts' AND name = 'kafka_executions_queue'")"
if [ "${actual_tpc:-0}" != "$TPC" ]; then
  echo "큐 설정(tpc=${actual_tpc:-0})이 인자($TPC)와 다름. 먼저 e3-set-tpc.sh $TPC" >&2
  rm -rf "$OUT"; exit 1
fi
docker compose -f "$ROOT/docker-compose.yml" ps order-server --format '{{.Health}}' | grep -q healthy \
  || { echo "order-server가 healthy가 아님" >&2; rm -rf "$OUT"; exit 1; }

# 같은 출발 상태: 열린 주문 0 (시뮬레이터가 앞 회차 주문을 다 처리할 때까지)
deadline=$(( $(date +%s) + PRE_DRAIN_SEC ))
while :; do
  open="$(mysql_q <<< "SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL');")"
  [ "$open" = "0" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { echo "시작 전 열린 주문이 ${open}건 남음" >&2; rm -rf "$OUT"; exit 1; }
  sleep 10
done

# --- lag 샘플러 (Kafka Admin API, 1초) ----------------------------------------
NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker run -d --name "$LAG_NAME" --network "$NET" -v "$TOOL:/app:ro" -v "$OUT:/out" \
  eclipse-temurin:21.0.12.1_1-jre-noble /app/bin/producer-tool lag \
  --groups clickhouse-executions:mts.trading.executions,clickhouse-orders:mts.trading.orders \
  --interval-ms 1000 --out /out/lag.csv >/dev/null
for _ in $(seq 30); do [ "$(lags)" != "-1,-1" ] && break; sleep 1; done
[ "$(lags)" != "-1,-1" ] || { echo "lag 샘플러가 값을 쓰지 않음: docker logs $LAG_NAME" >&2; exit 1; }
event "start tag=$TAG rate=$RATE warmup=${WARMUP_SEC}s stop=${STOP_SEC}s lags=$(lags)"

# --- 1초 샘플러 (백그라운드) ------------------------------------------------
PHASE_FILE="$OUT/.phase"; echo warmup > "$PHASE_FILE"
echo "ts_ms,phase,exec_lag,orders_lag,ch_mem_bytes" > "$OUT/samples.csv"
(
  while [ -e "$PHASE_FILE" ]; do
    t="$(now_ms)"
    echo "$t,$(cat "$PHASE_FILE" 2>/dev/null || echo end),$(lags),$(mem)" >> "$OUT/samples.csv"
    sleep "$(awk -v t="$t" -v n="$(now_ms)" 'BEGIN {s = 1 - (n - t) / 1000; print (s > 0 ? s : 0)}')"
  done
) &
SAMPLER=$!

cleanup() {
  rm -f "$PHASE_FILE"
  wait "$SAMPLER" 2>/dev/null || true
  docker rm -f "$K6_NAME" "$LAG_NAME" >/dev/null 2>&1 || true
  # 중간에 끊겨도 큐가 떼어진 채 남지 않게
  for q in kafka_executions_queue kafka_orders_queue; do
    [ "$(ch "EXISTS TABLE mts.$q")" = "1" ] || ch "ATTACH TABLE mts.$q"
  done
}
trap cleanup EXIT

# --- 부하 (중단·따라잡기 내내 유지) -------------------------------------------
LOAD_SEC=$(( WARMUP_SEC + STOP_SEC + MAX_CATCHUP_SEC + 120 ))
docker run -d --name "$K6_NAME" --network "$NET" -v "$ROOT/load-test:/scripts" grafana/k6:2.3.0 run --quiet \
  -e RATE="$RATE" -e DURATION="${LOAD_SEC}s" /scripts/orders.js >/dev/null
event "load started"
sleep "$WARMUP_SEC"

# --- 중단 ------------------------------------------------------------------
ch "DETACH TABLE mts.kafka_executions_queue"
ch "DETACH TABLE mts.kafka_orders_queue"
echo stopped > "$PHASE_FILE"; event "detached"
sleep "$STOP_SEC"

# --- 재개, 해소까지 대기 -------------------------------------------------------
echo catchup > "$PHASE_FILE"
ch "ATTACH TABLE mts.kafka_executions_queue"
ch "ATTACH TABLE mts.kafka_orders_queue"
ATTACH_MS="$(now_ms)"; event "attached"

resolved=""
deadline=$(( $(date +%s) + MAX_CATCHUP_SEC ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  IFS=, read -r e o <<< "$(lags)"
  if [ "$e" -ge 0 ] && [ "$o" -ge 0 ] && [ "$e" -le "$RESOLVE_LAG" ] && [ "$o" -le "$RESOLVE_LAG" ]; then resolved=1; break; fi
  sleep 1
done
if [ -n "$resolved" ]; then event "resolved exec_lag=$e orders_lag=$o"; else event "timeout exec_lag=$e orders_lag=$o"; fi

# 해소 후 30초 정상 상태를 남기고 부하 종료
echo steady > "$PHASE_FILE"; sleep 30
docker stop "$K6_NAME" >/dev/null; event "load stopped"
docker logs "$K6_NAME" > "$OUT/k6.log" 2>&1 || true

# 정합성은 움직임이 멈춘 뒤에 본다: 시뮬레이터가 열린 주문을 다 처리하고(주문 상태 고정) lag가 0이 될 때까지
echo drain > "$PHASE_FILE"
deadline=$(( $(date +%s) + OPEN_DRAIN_SEC ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  open="$(mysql_q <<< "SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL');")"
  [ "$open" = "0" ] && break; sleep 5
done
event "open orders=$open"
for _ in $(seq 120); do
  IFS=, read -r e o <<< "$(lags)"
  [ "$e" = "0" ] && [ "$o" = "0" ] && break; sleep 1
done
event "drained exec_lag=$e orders_lag=$o"
rm -f "$PHASE_FILE"; wait "$SAMPLER" 2>/dev/null || true
docker rm -f "$LAG_NAME" >/dev/null 2>&1 || true

# --- 정합성 ------------------------------------------------------------------
bash "$ROOT/experiments/scripts/reconcile-executions.sh" > "$OUT/reconcile-executions.txt" 2>&1 || true
bash "$ROOT/experiments/scripts/reconcile-orders.sh"     > "$OUT/reconcile-orders.txt" 2>&1 || true

# --- 요약 ------------------------------------------------------------------
awk -F, -v attach="$ATTACH_MS" -v thr="$RESOLVE_LAG" '
  function sec(t) { return (t == "" ? "-" : sprintf("%.1f", (t - attach) / 1000)) }
  NR == 1 { next }
  $2 == "stopped" || $2 == "catchup" { if ($3 > max_e) max_e = $3; if ($4 > max_o) max_o = $4 }
  $2 == "catchup" || $2 == "steady" {
    if ($5 > max_mem) max_mem = $5
    if ($1 >= attach) {
      # 첫 소비: 직전 샘플보다 lag가 줄어든 첫 시점 (큐마다 따로)
      if (fe == "" && pe != "" && $3 >= 0 && $3 < pe) fe = $1
      if (fo == "" && po != "" && $4 >= 0 && $4 < po) fo = $1
      if (re == "" && $3 >= 0 && $3 <= thr) re = $1
      if (ro == "" && $4 >= 0 && $4 <= thr) ro = $1
    }
  }
  { pe = $3; po = $4 }
  END {
    printf "lag 최고치 (체결/주문): %d / %d\n", max_e, max_o
    printf "첫 소비까지 (ATTACH 기준, 체결/주문): %s / %s초\n", sec(fe), sec(fo)
    printf "해소까지 (ATTACH 기준, lag <= %d, 체결/주문): %s / %s초\n", thr, sec(re), sec(ro)
    printf "따라잡기 중 ClickHouse MemoryTracking 최고: %.0f MiB\n", max_mem / 1048576
  }' "$OUT/samples.csv" | tee "$OUT/summary.txt"
grep -h "결과:" "$OUT"/reconcile-*.txt | tee -a "$OUT/summary.txt"
