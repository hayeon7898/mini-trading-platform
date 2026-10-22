#!/usr/bin/env bash
# E4 파티션 수와 처리량 — 백로그 소진 방식으로 ClickHouse Kafka 엔진의 최대 소비 처리량 측정
#
# 미리 채운 토픽 e4.p<P>에 새 컨슈머 그룹으로 Kafka 엔진 테이블 + MV를 붙이고,
# 대상 테이블 count()가 토픽 전체 건수에 도달할 때까지 1초 간격으로 기록한다.
#
# 사용법: bash experiments/scripts/e4-drain.sh <partitions> <consumers> <thread_per_consumer:0|1> <run>
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <partitions> <consumers> <thread_per_consumer:0|1> <run>" >&2
  exit 1
fi
P="$1"
C="$2"
TPC="$3"
RUN="$4"
TIMEOUT_SEC="${TIMEOUT_SEC:-900}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULT_DIR="$ROOT/experiments/results/20261022-E4-partition-throughput"
TOPIC="e4.p${P}"
TAG="p${P}-c${C}-tpc${TPC}-r${RUN}"
GROUP="e4-${TAG}"
OUT="$RESULT_DIR/raw/drain-${TAG}"

if [ -e "$OUT" ]; then
  echo "이미 결과가 있음: $OUT" >&2
  exit 1
fi
mkdir -p "$OUT"

now_ms() { date +%s%3N; }
log() { echo "[$(date -u +%H:%M:%S.%3N)] $*" | tee -a "$OUT/run.log"; }
ch() { docker exec mts-clickhouse clickhouse-client -n -q "$1"; }

N="$(docker exec mts-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9092 \
  --topic "$TOPIC" | awk -F: '{s += $3} END {print s}')"
if [ -z "$N" ] || [ "$N" = "0" ]; then
  echo "토픽 $TOPIC 가 비어 있음. 먼저 e4-prefill.sh $P" >&2
  exit 1
fi
log "tag=$TAG topic=$TOPIC group=$GROUP rows=$N consumers=$C thread_per_consumer=$TPC"

# 이전 실행 잔여물 정리 후 대상 테이블·큐 생성 (운영 테이블과 같은 스키마·설정)
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
"

# 컨테이너 CPU·메모리 표본 (병목 근거)
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

# MV를 만드는 순간 소비 시작
T0="$(now_ms)"
ch "
CREATE MATERIALIZED VIEW e4.mv TO e4.target AS
SELECT id, order_id, account_id, symbol, side,
       toDecimal64(price, 4)                                            AS price,
       quantity,
       fromUnixTimestamp64Milli(executed_at, 'UTC')                     AS executed_at,
       fromUnixTimestamp64Milli(__source_ts_ms, 'UTC')                  AS source_ts,
       toDateTime64(ifNull(_timestamp_ms, toDateTime64(0, 3)), 3, 'UTC') AS kafka_ts
FROM e4.queue;
"
log "mv created t0=$T0"

# rows: 대상 테이블에 보이는 행. 삽입은 약 100만 행 단위로 묶여(min_insert_block_size_rows) 계단식으로 보임
# read: 컨슈머가 Kafka에서 읽은 메시지 수 합계. 실시간으로 늘어나므로 소비 시작·진행은 이 값으로 판단
echo "elapsed_ms,read,rows" >"$OUT/drain.csv"
FIRST=""
END=""
ASSIGN_SAVED=""
while :; do
  line="$(ch "SELECT (SELECT sum(num_messages_read) FROM system.kafka_consumers WHERE database = 'e4'),
                     (SELECT count() FROM e4.target) FORMAT CSV")"
  read_msgs="${line%%,*}"
  rows="${line##*,}"
  t="$(now_ms)"
  echo "$((t - T0)),$read_msgs,$rows" >>"$OUT/drain.csv"
  if [ -z "$FIRST" ] && [ "${read_msgs:-0}" -gt 0 ]; then
    FIRST="$t"
    log "first read after $((FIRST - T0))ms"
  fi
  if [ -z "$ASSIGN_SAVED" ] && [ $((t - T0)) -gt 10000 ]; then
    ch "SELECT consumer_id, assignments.partition_id, num_messages_read, num_rebalance_assignments
        FROM system.kafka_consumers WHERE database = 'e4' FORMAT TSVWithNames" \
      >"$OUT/kafka-consumers-10s.tsv" 2>&1 || true
    ASSIGN_SAVED=1
  fi
  if [ "$rows" -ge "$N" ]; then
    END="$t"
    break
  fi
  # 1분이 지나도 0건이면 메시지 형식 오류 등으로 같은 위치를 재시도 중일 가능성 → 원인 기록 후 중단
  if [ -z "$FIRST" ] && [ $((t - T0)) -gt 60000 ]; then
    ch "SELECT consumer_id, num_messages_read, exceptions.text
        FROM system.kafka_consumers WHERE database = 'e4' FORMAT Vertical" \
      >"$OUT/kafka-exceptions.txt" 2>&1 || true
    log "NO_ROWS: 60초 동안 적재 0건 — kafka-exceptions.txt 확인"
    break
  fi
  if [ $((t - T0)) -gt $((TIMEOUT_SEC * 1000)) ]; then
    log "TIMEOUT: ${TIMEOUT_SEC}초 안에 다 소비하지 못함 rows=$rows"
    break
  fi
  sleep 1
done

kill "$STATS_PID" 2>/dev/null || true
ch "SELECT consumer_id, assignments.partition_id, num_messages_read, num_rebalance_assignments
    FROM system.kafka_consumers WHERE database = 'e4' FORMAT TSVWithNames" \
  >"$OUT/kafka-consumers-end.tsv" 2>&1 || true
ch "SELECT count() AS active_parts, sum(rows) AS rows FROM system.parts
    WHERE database = 'e4' AND table = 'target' AND active FORMAT TSVWithNames" >"$OUT/parts.tsv"
# 교차 확인용: ingested_at(DEFAULT now64)은 삽입 쿼리 시작 시각이라 행이 실제로 보인 시각보다 이를 수 있음
CH_SPAN="$(ch "SELECT dateDiff('millisecond', min(ingested_at), max(ingested_at)) FROM e4.target")"
ch "DROP TABLE e4.mv; DROP TABLE e4.queue; DROP TABLE e4.target;"

{
  echo "tag=$TAG"
  echo "partitions=$P"
  echo "consumers=$C"
  echo "thread_per_consumer=$TPC"
  echo "run=$RUN"
  echo "rows=$N"
  echo "t0_ms=$T0"
  echo "first_ms=${FIRST:-}"
  echo "end_ms=${END:-}"
  if [ -n "$FIRST" ] && [ -n "$END" ]; then
    # startup: MV 생성 → 첫 메시지 읽음 (그룹 참여·파티션 배정), drain: 첫 메시지 읽음 → 전 행 적재 완료
    echo "startup_ms=$((FIRST - T0))"
    echo "drain_ms=$((END - FIRST))"
    echo "throughput_rows_per_s=$((N * 1000 / (END - FIRST)))"
  fi
  echo "ch_ingest_span_ms=$CH_SPAN"
  if [ "${CH_SPAN:-0}" -gt 0 ]; then
    echo "ch_throughput_rows_per_s=$((N * 1000 / CH_SPAN))"
  fi
} | tee "$OUT/summary.txt"
log "done → $OUT"