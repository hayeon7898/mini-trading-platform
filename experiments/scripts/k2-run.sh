#!/usr/bin/env bash
# K2 깨진 메시지 적재 정지 실험 (#65, 07 스펙). 장애 재현 실험이다.
#
# 체결 큐를 정한 오류 처리 모드로 바꾸고, 80 TPS 부하 중 깨진 메시지 한 건을 넣은 뒤
# 적재가 멈추는지, 알림 조건이 충족되는지, 깨진 메시지가 어디에 남는지를 기록한다.
#
# 사용법: bash experiments/scripts/k2-run.sh <모드> <malformed|type> <회차>
#   모드: default | stream | stream-filtered | dead_letter_queue (k2-set-error-mode.sh 참고)
#   전제: producer-tool 빌드 (cd experiments/producer-tool && ./gradlew installDist)
#   환경변수: RATE(80) WARMUP_SEC(30) OBSERVE_SEC(300) SAMPLE_SEC(2) RECOVER_WAIT_SEC(120)
#             PROM_URL(http://localhost:9090) PRE_DRAIN_SEC(1800) OPEN_DRAIN_SEC(600)
#
# 기록 (<결과 폴더>/raw/<모드>-<종류>-r<회차>/)
#   marker.txt          : 깨진 메시지의 파티션, 오프셋, 주입 시각
#   samples.csv         : SAMPLE_SEC 간격 체결 행 수, 커밋 횟수 합, 예외가 있는 컨슈머 수, 알림 조건 값
#   lag-partitions.csv  : 1초 간격 파티션별 lag (producer-tool lag)
#   members.csv         : 파티션별 담당 컨슈머
#   kafka-consumers.txt : 관찰 끝 system.kafka_consumers (예외 문구 포함)
#   errors.txt          : 깨진 메시지가 남은 곳 (오류 테이블, DLQ)
#   events.log, summary.txt, reconcile.txt, k6.log
#
# 관찰이 끝나도 적재가 멈춰 있으면 dead_letter_queue 모드로 바꿔 깨진 메시지를 넘기고 회복 시간을 잰다.
# 실험을 모두 마친 뒤에는 k2-set-error-mode.sh restore로 되돌린다.
set -euo pipefail

export MSYS_NO_PATHCONV=1

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <default|stream|stream-filtered|dead_letter_queue> <malformed|type> <run>" >&2
  exit 1
fi
MODE="$1"; CASE="$2"; RUN="$3"
case "$MODE" in default|stream|stream-filtered|dead_letter_queue) ;; *) echo "알 수 없는 모드: $MODE" >&2; exit 1 ;; esac
case "$CASE" in malformed|type) ;; *) echo "malformed 또는 type이어야 함: $CASE" >&2; exit 1 ;; esac

RATE="${RATE:-80}"
WARMUP_SEC="${WARMUP_SEC:-30}"
OBSERVE_SEC="${OBSERVE_SEC:-300}"      # 알림은 정지 후 약 3분에 발생하므로 그보다 길게
SAMPLE_SEC="${SAMPLE_SEC:-2}"
RECOVER_WAIT_SEC="${RECOVER_WAIT_SEC:-120}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PRE_DRAIN_SEC="${PRE_DRAIN_SEC:-1800}"
OPEN_DRAIN_SEC="${OPEN_DRAIN_SEC:-600}"
RESULT_DIR="${RESULT_DIR:-experiments/results/20261105-K2-broken-message}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TAG="${MODE}-${CASE}-r${RUN}"
OUT="$ROOT/$RESULT_DIR/raw/${TAG}"
K6_NAME="k2-k6-${TAG}"
LAG_NAME="k2-lag-${TAG}"
TOOL="$ROOT/experiments/producer-tool/build/install/producer-tool"
[ -x "$TOOL/bin/producer-tool" ] || { echo "producer-tool 빌드 필요: (cd experiments/producer-tool && ./gradlew installDist)" >&2; exit 1; }
if [ -e "$OUT" ]; then echo "이미 결과가 있음: $OUT" >&2; exit 1; fi
mkdir -p "$OUT"

# shellcheck disable=SC2016  # 비밀번호 변수는 컨테이너 안에서 풀려야 한다
ch() { docker exec -i mts-clickhouse sh -c 'clickhouse-client --user "${CLICKHOUSE_USER:-default}" --password "$CLICKHOUSE_PASSWORD" "$@"' sh "$@"; }
# shellcheck disable=SC2016
mysql_q() { docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'; }
now_ms() { date +%s%3N; }
event() { echo "$(now_ms) $*" | tee -a "$OUT/events.log"; }
open_orders() { mysql_q <<< "SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL');"; }
exec_lag() { tail -n 1 "$OUT/lag.csv" 2>/dev/null | awk -F, '$1 ~ /^[0-9]+$/ {print $2; ok = 1} END {if (!ok) print -1}'; }

# 알림 #54의 조건 A와 같은 식 (1 = lag 1만 초과 + 2분간 커밋 변화 없음). 체결 토픽 값만 꺼낸다.
ALERT_EXPR='(sum by (topic) (kafka_topic_partition_current_offset{topic=~"mts\\.trading\\..+"}) - on (topic) sum by (topic) (kafka_consumergroup_current_offset_sum{consumergroup=~"clickhouse-.+"}) > bool 10000) * on (topic) (sum by (topic) (changes(kafka_consumergroup_current_offset_sum{consumergroup=~"clickhouse-.+"}[2m])) == bool 0)'
alert_value() {
  curl -sG --max-time 2 "$PROM_URL/api/v1/query" --data-urlencode "query=$ALERT_EXPR" 2>/dev/null \
    | grep -o '"topic":"mts.trading.executions"},"value":\[[^]]*\]' \
    | sed -E 's/.*,"([^"]*)"\]$/\1/' || true
}

# --- 사전 확인 -------------------------------------------------------------
ch -q "SELECT 1" > /dev/null || { echo "ClickHouse 접속 실패" >&2; rm -rf "$OUT"; exit 1; }
[ -n "$(alert_value)" ] || echo "경고: Prometheus($PROM_URL)에서 알림 조건 값을 읽지 못함. samples.csv의 alert 열이 비게 됨" >&2

deadline=$(( $(date +%s) + PRE_DRAIN_SEC ))
while :; do
  open="$(open_orders)"
  [ "$open" = "0" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { echo "시작 전 열린 주문이 ${open}건 남음" >&2; rm -rf "$OUT"; exit 1; }
  sleep 10
done

# --- 모드 전환 ---------------------------------------------------------------
bash "$ROOT/experiments/scripts/k2-set-error-mode.sh" "$MODE" | tee -a "$OUT/events.log"
sleep 10   # 컨슈머가 그룹에 들어올 시간

# --- lag 샘플러 -------------------------------------------------------------
NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker run -d --name "$LAG_NAME" --network "$NET" -v "$TOOL:/app:ro" -v "$OUT:/out" \
  eclipse-temurin:21.0.12.1_1-jre-noble /app/bin/producer-tool lag \
  --groups clickhouse-executions:mts.trading.executions,clickhouse-orders:mts.trading.orders \
  --interval-ms 1000 --out /out/lag.csv \
  --partitions-out /out/lag-partitions.csv --members-out /out/members.csv >/dev/null
for _ in $(seq 30); do [ "$(exec_lag)" != "-1" ] && break; sleep 1; done
event "start tag=$TAG rate=$RATE observe=${OBSERVE_SEC}s exec_lag=$(exec_lag)"

# --- 상태 샘플러 (백그라운드) -------------------------------------------------
RUN_FILE="$OUT/.running"; touch "$RUN_FILE"
echo "ts_ms,exec_rows,commits,consumers_with_exception,alert" > "$OUT/samples.csv"
(
  while [ -e "$RUN_FILE" ]; do
    t="$(now_ms)"
    row="$(ch --format CSV -q "SELECT
        (SELECT count() FROM mts.executions),
        (SELECT sum(num_commits) FROM system.kafka_consumers WHERE database = 'mts' AND table = 'kafka_executions_queue'),
        (SELECT countIf(length(exceptions.text) > 0) FROM system.kafka_consumers WHERE database = 'mts' AND table = 'kafka_executions_queue')" \
      2>/dev/null || echo "-1,-1,-1")"
    echo "$t,$row,$(alert_value)" >> "$OUT/samples.csv"
    sleep "$SAMPLE_SEC"
  done
) &
SAMPLER=$!

cleanup() {
  rm -f "$RUN_FILE"
  wait "$SAMPLER" 2>/dev/null || true
  docker rm -f "$K6_NAME" "$LAG_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- 부하와 주입 -------------------------------------------------------------
LOAD_SEC=$(( WARMUP_SEC + OBSERVE_SEC + 60 ))
docker run -d --name "$K6_NAME" --network "$NET" -v "$ROOT/load-test:/scripts" grafana/k6:2.3.0 run --quiet \
  -e RATE="$RATE" -e DURATION="${LOAD_SEC}s" /scripts/orders.js >/dev/null
event "load started"
sleep "$WARMUP_SEC"

bash "$ROOT/experiments/scripts/k2-inject.sh" "$CASE" "$OUT" > /dev/null
INJECT_MS="$(grep '^inject_ms=' "$OUT/marker.txt" | cut -d= -f2)"
M_PART="$(grep '^partition=' "$OUT/marker.txt" | cut -d= -f2)"
M_OFF="$(grep '^offset=' "$OUT/marker.txt" | cut -d= -f2)"
event "injected case=$CASE partition=$M_PART offset=$M_OFF"

sleep "$OBSERVE_SEC"
OBSERVE_END_MS="$(now_ms)"; event "observe end"

# --- 관찰 끝 상태 -------------------------------------------------------------
ch --format Vertical -q "SELECT consumer_id, assignments.partition_id, assignments.current_offset,
    last_commit_time, num_commits, exceptions.time, exceptions.text
  FROM system.kafka_consumers WHERE database = 'mts' AND table = 'kafka_executions_queue'" \
  > "$OUT/kafka-consumers.txt" 2>&1 || true

{
  echo "== 대상 테이블의 표식, 기본값 행 (id = 0 또는 표식 id)"
  ch -q "SELECT id, symbol, quantity, executed_at FROM mts.executions WHERE id = 0 OR id >= 900000000" 2>&1 || true
  if [ "$MODE" = "stream-filtered" ]; then
    echo "== mts.k2_execution_errors"
    ch --format Vertical -q "SELECT * FROM mts.k2_execution_errors ORDER BY received_at" 2>&1 || true
  fi
  if [ "$MODE" = "dead_letter_queue" ]; then
    ch -q "SYSTEM FLUSH LOGS" > /dev/null 2>&1 || true
    echo "== system.dead_letter_queue"
    ch --format Vertical -q "SELECT * FROM system.dead_letter_queue
      WHERE database = 'mts' AND table = 'kafka_executions_queue' ORDER BY event_time DESC LIMIT 5" 2>&1 || true
  fi
} > "$OUT/errors.txt"

docker stop "$K6_NAME" >/dev/null; event "load stopped"
docker logs "$K6_NAME" > "$OUT/k6.log" 2>&1 || true

# --- 회복 ------------------------------------------------------------------
# 부하를 멈춘 뒤에도 체결 lag가 남아 있으면 적재가 멈춘 것으로 보고 DLQ 모드로 넘긴다.
recovered="not needed"
for _ in $(seq "$RECOVER_WAIT_SEC"); do [ "$(exec_lag)" = "0" ] && break; sleep 1; done
if [ "$(exec_lag)" != "0" ]; then
  event "still stalled exec_lag=$(exec_lag), switching to dead_letter_queue"
  rec_start="$(now_ms)"
  bash "$ROOT/experiments/scripts/k2-set-error-mode.sh" dead_letter_queue | tee -a "$OUT/events.log"
  for _ in $(seq 300); do [ "$(exec_lag)" = "0" ] && break; sleep 1; done
  recovered="$(awk -v s="$rec_start" -v e="$(now_ms)" 'BEGIN {printf "%.1f초 (DLQ 모드 전환 후 lag 0까지)", (e - s) / 1000}')"
  event "recovered exec_lag=$(exec_lag) in $recovered"
fi

deadline=$(( $(date +%s) + OPEN_DRAIN_SEC ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  open="$(open_orders)"; [ "$open" = "0" ] && break; sleep 5
done
for _ in $(seq 120); do [ "$(exec_lag)" = "0" ] && break; sleep 1; done
event "drained open_orders=$open exec_lag=$(exec_lag)"
rm -f "$RUN_FILE"; wait "$SAMPLER" 2>/dev/null || true
docker rm -f "$LAG_NAME" >/dev/null 2>&1 || true

# --- 정합성 ------------------------------------------------------------------
my="$(mysql_q <<< "SELECT COUNT(*) FROM executions;")"
chc="$(ch -q "SELECT count() FROM mts.executions FINAL WHERE id != 0 AND id < 900000000")"
junk="$(ch -q "SELECT count() FROM mts.executions WHERE id = 0 OR id >= 900000000")"
{
  echo "MySQL 체결: $my"
  echo "ClickHouse 체결 (표식, 기본값 행 제외): $chc"
  echo "ClickHouse 표식, 기본값 행: $junk"
  if [ "$my" = "$chc" ]; then echo "결과: 일치"; else echo "결과: 불일치"; fi
} > "$OUT/reconcile.txt"

# --- 요약 ------------------------------------------------------------------
{
  echo "tag=$TAG marker_partition=$M_PART marker_offset=$M_OFF observe=${OBSERVE_SEC}s"

  # 표식 파티션과 같은 컨슈머의 파티션, 나머지 파티션의 lag와 커밋 진행
  awk -F, -v s="$INJECT_MS" -v e="$OBSERVE_END_MS" -v mp="$M_PART" -v mo="$M_OFF" '
    NR == 1 || $2 != "clickhouse-executions" || $1 < s || $1 > e { next }
    {
      p = $3; if ($6 > maxlag[p]) maxlag[p] = $6
      last_commit[p] = $5; seen[p] = 1
    }
    END {
      for (p in seen) {
        if (p == mp) {
          printf "표식 파티션 %d: 최대 lag %d, 관찰 끝 커밋 오프셋 %d (표식 %d) → %s\n",
            p, maxlag[p], last_commit[p], mo, (last_commit[p] <= mo ? "표식에서 멈춤" : "표식을 지나감")
        } else if (maxlag[p] > om) { om = maxlag[p]; op = p }
      }
      printf "나머지 파티션 중 최대 lag: 파티션 %d, %d\n", op, om
    }' "$OUT/lag-partitions.csv"

  awk -F, -v s="$INJECT_MS" '
    NR == 1 || $1 < s { next }
    {
      if (first == "") first = $2
      last = $2
      if ($4 > maxexc) maxexc = $4
      if ($5 == "1") { run += 1; if (run > maxrun) maxrun = run; hits++ } else run = 0
      if ($5 != "") known++
    }
    END {
      printf "관찰 중 체결 행 증가: %d\n", last - first
      printf "예외가 있는 컨슈머 수 최대: %d\n", maxexc
      if (known == 0) print "알림 조건: Prometheus 값 없음"
      else printf "알림 조건 충족 샘플: %d / %d, 가장 긴 연속 %d개\n", hits + 0, known, maxrun + 0
    }' "$OUT/samples.csv"

  echo "회복: $recovered"
  cat "$OUT/reconcile.txt"
} | tee "$OUT/summary.txt"