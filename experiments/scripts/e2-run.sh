#!/usr/bin/env bash
# E2 Debezium 커넥터 강제 종료와 중복 (장애 재현 실험) — 1회 실행
#
# 주문 부하 중 FAIL_AT초에 Kafka Connect를 kill(강제) 또는 stop(정상 종료) → DOWN_SEC초 뒤 재기동.
# 실험 구간의 토픽 오프셋 범위, 오프셋 저장(connect-offsets) 시각, 복구 시간, 정합성 대조를 남긴다.
# 중복 집계는 e2-dup.sh <결과 폴더>.
#
# 사용법: bash experiments/scripts/e2-run.sh <kill|stop> <run>
# 환경변수(선택): RATE=300 LOAD_SEC=180 FAIL_AT=90 DOWN_SEC=15 TAG=
set -euo pipefail

if [ "$#" -ne 2 ] || { [ "$1" != "kill" ] && [ "$1" != "stop" ]; }; then
  echo "usage: $0 <kill|stop> <run>" >&2
  exit 1
fi
MODE="$1"
RUN="$2"
RATE="${RATE:-300}"
LOAD_SEC="${LOAD_SEC:-180}"
FAIL_AT="${FAIL_AT:-90}"
DOWN_SEC="${DOWN_SEC:-15}"
TAG="${TAG:-}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULT_DIR="$ROOT/experiments/results/20261023-E2-connector-restart-dup"   # 날짜가 바뀌어도 경로 고정
NAME="${MODE}${TAG:+-$TAG}-r${RUN}"
OUT="$RESULT_DIR/raw/$NAME"
CONNECTOR="mts-trading-connector"
TOPICS='mts\.trading\.(orders|executions)'
NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"

if [ -e "$OUT" ]; then
  echo "이미 결과가 있음: $OUT" >&2
  exit 1
fi
mkdir -p "$OUT"
cd "$ROOT"

now_ms() { date +%s%3N; }
log() { echo "[$(date -u +%H:%M:%S.%3N)] $*" | tee -a "$OUT/run.log"; }
end_offsets() {
  docker exec mts-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9092 --topic "$TOPICS"
}
sum_offsets() { awk -F: '{s += $3} END {print s + 0}'; }
connector_state() {
  curl -s --max-time 3 "localhost:8083/connectors/$CONNECTOR/status" 2>/dev/null |
    jq -r '[.connector.state, (.tasks[0].state // "NONE")] | join("/")' 2>/dev/null || echo "DOWN"
}

# --- 0. 사전 조건 -------------------------------------------------------------
if [ "$(connector_state)" != "RUNNING/RUNNING" ]; then
  echo "커넥터가 RUNNING이 아님: $(connector_state)" >&2
  exit 1
fi
URP="$(docker exec mts-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --describe --under-replicated-partitions | grep -c 'Partition:' || true)"
if [ "$URP" != "0" ]; then
  echo "URP $URP 개 — 복구 후 실행" >&2
  exit 1
fi

# --- 1. 실험 구간 시작 오프셋 -------------------------------------------------------
end_offsets >"$OUT/offsets-start.txt"
START_MS="$(now_ms)"
log "mode=$MODE run=$RUN rate=$RATE load_sec=$LOAD_SEC fail_at=$FAIL_AT down_sec=$DOWN_SEC"
log "start offsets: $(sum_offsets <"$OUT/offsets-start.txt") (orders+executions)"

# 토픽 끝 오프셋 표본 (발행 속도·재개 시각 근거)
(
  echo "epoch_ms,end_offset_sum"
  while :; do
    echo "$(now_ms),$(end_offsets | sum_offsets)"
    sleep 0.5
  done
) >"$OUT/offsets-timeline.csv" 2>/dev/null &
SAMPLER_PID=$!
trap 'kill "$SAMPLER_PID" 2>/dev/null || true' EXIT

# --- 2. 주문 부하 -------------------------------------------------------------
docker run --rm -i --network "$NET" -v "$ROOT/load-test:/scripts" grafana/k6:2.3.0 run --quiet \
  -e BASE_URL=http://mts-order-server:8080 -e RATE="$RATE" -e DURATION="${LOAD_SEC}s" /scripts/orders.js \
  >"$OUT/k6.log" 2>&1 &
K6_PID=$!

# --- 3. 장애 --------------------------------------------------------------------
sleep "$FAIL_AT"
BEFORE_FAIL="$(end_offsets | sum_offsets)"
FAIL_MS="$(now_ms)"
if [ "$MODE" = "kill" ]; then
  docker kill mts-connect >/dev/null
else
  docker stop -t 30 mts-connect >/dev/null
fi
STOPPED_MS="$(now_ms)"
log "$MODE mts-connect fail_ms=$FAIL_MS (stopped after $((STOPPED_MS - FAIL_MS))ms) end_offsets=$BEFORE_FAIL"

sleep "$DOWN_SEC"
AT_RESTART="$(end_offsets | sum_offsets)"
RESTART_MS="$(now_ms)"
docker start mts-connect >/dev/null
log "start mts-connect restart_ms=$RESTART_MS end_offsets=$AT_RESTART"

# --- 4. 복구: RUNNING, 발행 재개 ---------------------------------------------------
RUNNING_MS=""
RESUME_MS=""
for _ in $(seq 1 360); do
  if [ -z "$RUNNING_MS" ] && [ "$(connector_state)" = "RUNNING/RUNNING" ]; then
    RUNNING_MS="$(now_ms)"
    log "connector RUNNING after restart +$((RUNNING_MS - RESTART_MS))ms"
  fi
  if [ -z "$RESUME_MS" ] && [ "$(end_offsets | sum_offsets)" -gt "$AT_RESTART" ]; then
    RESUME_MS="$(now_ms)"
    log "publishing resumed after restart +$((RESUME_MS - RESTART_MS))ms"
  fi
  [ -n "$RUNNING_MS" ] && [ -n "$RESUME_MS" ] && break
  sleep 0.5
done
[ -z "$RESUME_MS" ] && log "WARN: 3분 동안 발행 재개 확인 못 함 (상태 $(connector_state))"

# --- 5. 부하 종료·따라잡기 -------------------------------------------------------
wait "$K6_PID" || log "k6 exited with $?"
log "load done"
# 체결 시뮬레이터가 남은 접수 주문을 처리하고 CDC가 따라잡을 때까지: 끝 오프셋이 10초간 그대로면 종료
prev=-1
stable=0
for _ in $(seq 1 120); do
  cur="$(end_offsets | sum_offsets)"
  if [ "$cur" = "$prev" ]; then
    stable=$((stable + 1))
    [ "$stable" -ge 5 ] && break
  else
    stable=0
  fi
  prev="$cur"
  sleep 2
done
kill "$SAMPLER_PID" 2>/dev/null || true
end_offsets >"$OUT/offsets-end.txt"
END_MS="$(now_ms)"
log "end offsets: $(sum_offsets <"$OUT/offsets-end.txt") (+$(($(sum_offsets <"$OUT/offsets-end.txt") - $(sum_offsets <"$OUT/offsets-start.txt"))) in window)"

# --- 6. 오프셋 저장 기록 (connect-offsets), Connect 로그 --------------------------------
docker exec mts-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 \
  --topic connect-offsets --from-beginning --timeout-ms 15000 \
  --formatter-property print.timestamp=true --formatter-property print.key=true 2>/dev/null |
  grep -F "$CONNECTOR" |
  awk -v s="$START_MS" '{split($1, a, ":"); if (a[2] + 0 >= s) print}' >"$OUT/connect-offsets.txt" || true
log "offset commits in window: $(wc -l <"$OUT/connect-offsets.txt")"
docker logs --since "$((START_MS / 1000))" mts-connect >"$OUT/connect.log" 2>&1 || true

# --- 7. 정합성 대조 ---------------------------------------------------------------
bash experiments/scripts/reconcile-executions.sh >"$OUT/reconcile-executions.txt" 2>&1 || true
bash experiments/scripts/reconcile-orders.sh >"$OUT/reconcile-orders.txt" 2>&1 || true
tail -5 "$OUT/reconcile-executions.txt" "$OUT/reconcile-orders.txt"

{
  echo "mode=$MODE"
  echo "run=$RUN"
  echo "tag=$TAG"
  echo "rate=$RATE"
  echo "load_sec=$LOAD_SEC"
  echo "fail_at=$FAIL_AT"
  echo "down_sec=$DOWN_SEC"
  echo "start_ms=$START_MS"
  echo "fail_ms=$FAIL_MS"
  echo "stopped_ms=$STOPPED_MS"
  echo "restart_ms=$RESTART_MS"
  echo "running_ms=${RUNNING_MS:-}"
  echo "resume_ms=${RESUME_MS:-}"
  echo "end_ms=$END_MS"
  echo "end_offsets_before_fail=$BEFORE_FAIL"
  echo "end_offsets_at_restart=$AT_RESTART"
} >"$OUT/meta.txt"
log "done → $OUT  (다음: bash experiments/scripts/e2-dup.sh $OUT)"
