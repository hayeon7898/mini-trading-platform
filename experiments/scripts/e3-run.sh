#!/usr/bin/env bash
# E3 컨슈머 중단 후 따라잡기 (#43)
#
# 500 TPS 부하를 건 상태에서 운영 큐 두 개를 DETACH → STOP분 대기 → ATTACH 하고,
# 부하를 계속 넣으면서 lag가 해소될 때까지 1초 간격으로 기록한다. 끝나면 부하를 멈추고 정합성을 대조한다.
#
# 사용법: bash experiments/scripts/e3-run.sh <tpc:0|1> <stop_min> <run>
#   전제: e3-set-tpc.sh <tpc> 로 큐 설정을 맞춘 상태, 두 컨슈머 그룹 lag가 작을 것
#   환경변수: RATE(500) WARMUP_SEC(60) STOP_SEC(stop_min*60, 시험 실행용) RESOLVE_LAG(10000) MAX_CATCHUP_SEC(900)
#
# 기록 (raw/<tag>/)
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
RATE="${RATE:-500}"
WARMUP_SEC="${WARMUP_SEC:-60}"
STOP_SEC="${STOP_SEC:-$(( STOP_MIN * 60 ))}"
RESOLVE_LAG="${RESOLVE_LAG:-10000}"     # 묶음 1개(kafka_max_block_size) 이하면 해소로 본다
MAX_CATCHUP_SEC="${MAX_CATCHUP_SEC:-900}"
EXPORTER="${EXPORTER:-http://localhost:9308/metrics}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TAG="tpc${TPC}-stop${STOP_MIN}m-r${RUN}"
OUT="$ROOT/experiments/results/20261028-E3-consumer-catchup/raw/${TAG}"
K6_NAME="e3-k6-${TAG}"

if [ -e "$OUT" ]; then echo "이미 결과가 있음: $OUT" >&2; exit 1; fi
mkdir -p "$OUT"

ch() { docker exec mts-clickhouse clickhouse-client -q "$1"; }
now_ms() { date +%s%3N; }
event() { echo "$(now_ms) $*" | tee -a "$OUT/events.log"; }

# 컨슈머 그룹 lag 합계 (kafka-exporter는 요청마다 Kafka에서 새로 읽는다. JVM 도구는 1회 2~3초라 1초 간격에 못 씀)
lags() {
  curl -s "$EXPORTER" | awk '
    /^kafka_consumergroup_lag_sum\{consumergroup="clickhouse-executions"/ {e += $NF; se = 1}
    /^kafka_consumergroup_lag_sum\{consumergroup="clickhouse-orders"/     {o += $NF; so = 1}
    END {printf "%d,%d", (se ? e : -1), (so ? o : -1)}'   # 값을 못 읽으면 -1 (0으로 오인해 해소 판정하지 않게)
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
  docker rm -f "$K6_NAME" >/dev/null 2>&1 || true
  # 중간에 끊겨도 큐가 떼어진 채 남지 않게
  for q in kafka_executions_queue kafka_orders_queue; do
    [ "$(ch "EXISTS TABLE mts.$q")" = "1" ] || ch "ATTACH TABLE mts.$q"
  done
}
trap cleanup EXIT

# --- 부하 (중단·따라잡기 내내 유지) -------------------------------------------
NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
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
echo drain > "$PHASE_FILE"; sleep 20
rm -f "$PHASE_FILE"; wait "$SAMPLER" 2>/dev/null || true

# --- 정합성 ------------------------------------------------------------------
bash "$ROOT/experiments/scripts/reconcile-executions.sh" > "$OUT/reconcile-executions.txt" 2>&1 || true
bash "$ROOT/experiments/scripts/reconcile-orders.sh"     > "$OUT/reconcile-orders.txt" 2>&1 || true

# --- 요약 ------------------------------------------------------------------
awk -F, -v attach="$ATTACH_MS" -v thr="$RESOLVE_LAG" '
  NR == 1 { next }
  $2 == "stopped" || $2 == "catchup" { if ($3 > max_e) max_e = $3; if ($4 > max_o) max_o = $4 }
  $2 == "catchup" || $2 == "steady" {
    if ($5 > max_mem) max_mem = $5
    if ($1 >= attach && first == "" && prev_e != "" && $3 < prev_e) first = $1
    if ($1 >= attach && res == "" && $3 >= 0 && $4 >= 0 && $3 <= thr && $4 <= thr) res = $1
  }
  { prev_e = $3 }
  END {
    printf "lag 최고치 (체결/주문): %d / %d\n", max_e, max_o
    printf "첫 소비까지 (ATTACH 기준): %s초\n", (first == "" ? "-" : sprintf("%.1f", (first - attach) / 1000))
    printf "해소까지 (ATTACH 기준, lag <= %d): %s초\n", thr, (res == "" ? "-" : sprintf("%.1f", (res - attach) / 1000))
    printf "따라잡기 중 ClickHouse MemoryTracking 최고: %.0f MiB\n", max_mem / 1048576
  }' "$OUT/samples.csv" | tee "$OUT/summary.txt"
grep -h "결과:" "$OUT"/reconcile-*.txt | tee -a "$OUT/summary.txt"
