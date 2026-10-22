#!/usr/bin/env bash
# E4 파티션 수와 처리량 — 생산과 소비가 동시에 도는 상태에서 lag가 쌓이기 시작하는 지점 측정
#
# ClickHouse Kafka 엔진을 먼저 붙여 둔 뒤 프로듀서 속도를 단계적으로 올린다.
# 단계마다 STEP_SEC초 전송, 이후 백로그가 다 소진될 때까지 기다리고, kafka-exporter 지표로 lag·생산·소비 속도를 남긴다.
#
# 사용법: bash experiments/scripts/e4-step.sh <partitions> <consumers> <thread_per_consumer:0|1>
# 환경변수(선택): RATES="50000 100000 150000 200000" STEP_SEC=60
#                 SUFFIX=high  같은 조합을 다른 단계로 다시 돌릴 때 폴더·토픽 이름 구분
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <partitions> <consumers> <thread_per_consumer:0|1>" >&2
  exit 1
fi
P="$1"
C="$2"
TPC="$3"
RATES="${RATES:-50000 100000 150000 200000}"
STEP_SEC="${STEP_SEC:-60}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULT_DIR="$ROOT/experiments/results/20261022-E4-partition-throughput"
SUFFIX="${SUFFIX:-}"
TAG="step-p${P}-c${C}-tpc${TPC}${SUFFIX:+-$SUFFIX}"
TOPIC="e4.${TAG}"
GROUP="e4-${TAG}-$(date +%s)"
OUT="$RESULT_DIR/raw/${TAG}"
TOOL="$ROOT/experiments/producer-tool/build/install/producer-tool"
NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
KT=(docker exec mts-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092)

if [ -e "$OUT" ]; then
  echo "이미 결과가 있음: $OUT" >&2
  exit 1
fi
mkdir -p "$OUT"

now_ms() { date +%s%3N; }
log() { echo "[$(date -u +%H:%M:%S.%3N)] $*" | tee -a "$OUT/run.log"; }
ch() { docker exec mts-clickhouse clickhouse-client -n -q "$1"; }

"${KT[@]}" --create --topic "$TOPIC" --partitions "$P" --replication-factor 3 >/dev/null
log "tag=$TAG topic=$TOPIC group=$GROUP rates=[$RATES] step_sec=$STEP_SEC"

ch "
DROP TABLE IF EXISTS e4.mv;
DROP TABLE IF EXISTS e4.queue;
DROP TABLE IF EXISTS e4.target;
CREATE DATABASE IF NOT EXISTS e4;
CREATE TABLE e4.target
(
    id          UInt64,
    order_id    UInt64,
    account_id  UInt64,
    symbol      LowCardinality(String),
    side        LowCardinality(String),
    price       Decimal(18, 4),
    quantity    UInt32,
    executed_at DateTime64(3, 'UTC'),
    source_ts   DateTime64(3, 'UTC'),
    kafka_ts    DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(executed_at)
ORDER BY id;
CREATE TABLE e4.queue
(
    id UInt64, order_id UInt64, account_id UInt64, symbol String, side String, price String,
    quantity UInt32, executed_at Int64, __op String, __source_ts_ms Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = '${TOPIC}',
         kafka_group_name = '${GROUP}',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = ${C},
         kafka_disable_num_consumers_limit = 1,
         kafka_thread_per_consumer = ${TPC},
         kafka_max_block_size = 10000,
         kafka_poll_max_batch_size = 10000,
         kafka_flush_interval_ms = 1000;
CREATE MATERIALIZED VIEW e4.mv TO e4.target AS
SELECT id, order_id, account_id, symbol, side,
       toDecimal64(price, 4)                                            AS price,
       quantity,
       fromUnixTimestamp64Milli(executed_at, 'UTC')                     AS executed_at,
       fromUnixTimestamp64Milli(__source_ts_ms, 'UTC')                  AS source_ts,
       toDateTime64(ifNull(_timestamp_ms, toDateTime64(0, 3)), 3, 'UTC') AS kafka_ts
FROM e4.queue;
"

(
  echo "epoch_ms,name,cpu,mem"
  while :; do
    t="$(now_ms)"
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' |
      grep '^mts-' | sed "s/^/$t,/"
  done
) >"$OUT/stats.csv" 2>/dev/null &
STATS_PID=$!
trap 'kill "$STATS_PID" 2>/dev/null || true' EXIT

# 그룹 참여·파티션 배정이 끝날 시간
sleep 15

START_MS="$(now_ms)"
echo "rate,start_ms,end_ms" >"$OUT/steps.csv"
TOTAL=0
ID_OFFSET=0
for rate in $RATES; do
  s="$(now_ms)"
  log "step rate=$rate start"
  mkdir -p "$OUT/producer-$rate"
  docker run --rm --network "$NET" -v "$TOOL:/app:ro" -v "$OUT/producer-$rate:/out" eclipse-temurin:21-jre \
    /app/bin/producer-tool produce --topic "$TOPIC" --format execution --key id --record-each false \
    --acks all --idempotence true --rate "$rate" --duration "$STEP_SEC" --id-offset "$ID_OFFSET" \
    --producer-config linger.ms=20,batch.size=262144,compression.type=lz4 \
    >"$OUT/producer-$rate.log" 2>&1
  e="$(now_ms)"
  echo "$rate,$s,$e" >>"$OUT/steps.csv"
  # 목표(rate × STEP_SEC)가 아니라 실제 ack된 건수로 누적: 프로듀서가 못 따라가거나 실패하면 둘이 다름
  acked="$(grep -h '^acked=' "$OUT/producer-$rate/summary.txt" | cut -d= -f2)"
  TOTAL=$((TOTAL + ${acked:-0}))
  # 단계마다 id 구간을 이어 붙여 대상 테이블(ReplacingMergeTree)에서 합쳐지지 않게 함
  ID_OFFSET=$((ID_OFFSET + rate * STEP_SEC))
  log "step rate=$rate end ($((e - s))ms) $(grep -o 'done attempted.*' "$OUT/producer-$rate.log" | head -1)"
done

# 남은 백로그 소진 대기 (최대 5분)
END_MS=""
for _ in $(seq 1 300); do
  rows="$(ch "SELECT count() FROM e4.target")"
  if [ "$rows" -ge "$TOTAL" ]; then
    END_MS="$(now_ms)"
    break
  fi
  sleep 1
done
[ -z "$END_MS" ] && END_MS="$(now_ms)" && log "TIMEOUT: rows=$rows / $TOTAL"
log "drained acked_total=$TOTAL after_last_step_ms=$((END_MS - $(tail -1 "$OUT/steps.csv" | cut -d, -f3)))"
kill "$STATS_PID" 2>/dev/null || true

# 지표 (kafka-exporter, 5초 해상도)
sleep 15
FROM=$((START_MS / 1000 - 10))
TO=$((END_MS / 1000 + 15))
PR="$ROOT/experiments/scripts/prom-range.sh"
bash "$PR" "sum(kafka_consumergroup_lag_sum{consumergroup=\"$GROUP\"})" "$FROM" "$TO" 5s >"$OUT/lag.csv"
bash "$PR" "sum(rate(kafka_topic_partition_current_offset{topic=\"$TOPIC\"}[30s]))" "$FROM" "$TO" 5s >"$OUT/produce-rate.csv"
bash "$PR" "sum(rate(kafka_consumergroup_current_offset{consumergroup=\"$GROUP\"}[30s]))" "$FROM" "$TO" 5s >"$OUT/consume-rate.csv"

ch "DROP TABLE e4.mv; DROP TABLE e4.queue; DROP TABLE e4.target;"
{
  echo "tag=$TAG"
  echo "topic=$TOPIC"
  echo "group=$GROUP"
  echo "rates=$RATES"
  echo "step_sec=$STEP_SEC"
  echo "total=$TOTAL"
  echo "start_ms=$START_MS"
  echo "end_ms=$END_MS"
} >"$OUT/summary.txt"
log "done → $OUT"