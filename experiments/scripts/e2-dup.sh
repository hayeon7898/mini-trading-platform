#!/usr/bin/env bash
# E2 — 실험 구간(시작~끝 오프셋)의 CDC 메시지에서 중복을 집계한다.
#
# ClickHouse 임시 Kafka 엔진(RawBLOB)으로 두 토픽을 새 컨슈머 그룹으로 처음부터 읽고,
# 실험 구간 오프셋만 남겨 (토픽, 키, 원문)이 같은 메시지를 중복으로 센다.
# 재발행은 키·원문이 바이트 단위로 같으므로 (id, __op, __source_ts_ms)보다 정확하다.
#
# 사용법: bash experiments/scripts/e2-dup.sh <e2-run.sh 결과 폴더>
set -euo pipefail

if [ "$#" -ne 1 ] || [ ! -f "$1/offsets-start.txt" ] || [ ! -f "$1/offsets-end.txt" ]; then
  echo "usage: $0 <e2-run.sh 결과 폴더 (offsets-start.txt, offsets-end.txt 필요)>" >&2
  exit 1
fi
OUT="$1"
GROUP="e2-dup-$(basename "$OUT")-$(date +%s)"

ch() { docker exec mts-clickhouse clickhouse-client -n -q "$1"; }
log() { echo "[$(date -u +%H:%M:%S)] $*"; }

# 실험 구간: 토픽·파티션별 [시작, 끝) 오프셋 (토픽#파티션:시작:끝)
RANGES="$(join -t: -j1 \
  <(awk -F: '{print $1"#"$2":"$3}' "$OUT/offsets-start.txt" | sort) \
  <(awk -F: '{print $1"#"$2":"$3}' "$OUT/offsets-end.txt" | sort))"
WINDOW="$(echo "$RANGES" |
  awk -F'[#:]' '{printf "%s(_topic = '\''%s'\'' AND _partition = %s AND _offset >= %s AND _offset < %s)", (NR > 1 ? " OR " : ""), $1, $2, $3, $4}')"
EXPECTED="$(echo "$RANGES" | awk -F'[#:]' '{s += $4 - $3} END {print s}')"
log "group=$GROUP expected_in_window=$EXPECTED"

ch "
DROP TABLE IF EXISTS e2.mv;
DROP TABLE IF EXISTS e2.queue;
DROP TABLE IF EXISTS e2.msgs;
CREATE DATABASE IF NOT EXISTS e2;
CREATE TABLE e2.msgs
(
    topic     LowCardinality(String),
    partition UInt32,
    offset    UInt64,
    ts        DateTime64(3, 'UTC'),
    key       String,
    raw       String
)
-- 같은 오프셋을 ClickHouse가 다시 읽는 경우(at-least-once)는 Kafka 중복이 아니므로 오프셋 단위로 합침
ENGINE = ReplacingMergeTree ORDER BY (topic, partition, offset);
CREATE TABLE e2.queue (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'mts.trading.orders,mts.trading.executions',
         kafka_group_name = '${GROUP}',
         kafka_format = 'RawBLOB',
         kafka_num_consumers = 3,
         kafka_thread_per_consumer = 1,
         kafka_max_block_size = 10000,
         kafka_flush_interval_ms = 1000;
CREATE MATERIALIZED VIEW e2.mv TO e2.msgs AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       toDateTime64(ifNull(_timestamp_ms, toDateTime64(0, 3)), 3, 'UTC') AS ts,
       ifNull(_key, '') AS key, raw
FROM e2.queue
WHERE ${WINDOW};
"

# 구간 메시지가 다 들어올 때까지 (토픽 전체를 처음부터 읽으므로 수십 초 걸릴 수 있음)
rows=0
for _ in $(seq 1 600); do
  rows="$(ch "SELECT uniqExact(topic, partition, offset) FROM e2.msgs")"
  [ "$rows" -ge "$EXPECTED" ] && break
  sleep 1
done
log "loaded rows=$rows / expected=$EXPECTED"
ch "DROP TABLE e2.mv; DROP TABLE e2.queue;"

# 집계: 중복 = 오프셋은 다른데 (토픽, 키, 원문)이 같은 메시지
ch "
SELECT topic,
       count()                         AS total,
       uniqExact(key, raw)             AS distinct_msgs,
       total - distinct_msgs           AS duplicates
FROM e2.msgs FINAL GROUP BY topic ORDER BY topic
FORMAT PrettyCompactMonoBlock" | tee "$OUT/dup-summary.txt"

# 중복 묶음별: 원본(첫 등장)과 재발행(마지막 등장)의 시각·오프셋
ch "
SELECT topic, key, count() AS c,
       min(ts) AS first_ts, max(ts) AS last_ts,
       min(offset) AS first_offset, max(offset) AS last_offset
FROM e2.msgs FINAL GROUP BY topic, key, raw HAVING c > 1
ORDER BY topic, first_ts
FORMAT TSVWithNames" >"$OUT/dup-groups.tsv"

ch "
SELECT topic,
       count()        AS dup_groups,
       max(c)         AS max_copies,
       min(first_ts)  AS originals_from,
       max(first_ts)  AS originals_to,
       min(last_ts)   AS resent_from,
       max(last_ts)   AS resent_to
FROM (
    SELECT topic, count() AS c, min(ts) AS first_ts, max(ts) AS last_ts
    FROM e2.msgs FINAL GROUP BY topic, key, raw HAVING c > 1
)
GROUP BY topic ORDER BY topic
FORMAT PrettyCompactMonoBlock" | tee -a "$OUT/dup-summary.txt"

# 발행 재개 시각: 재기동 이후 첫 메시지의 Kafka 타임스탬프 (e2-run.sh의 CLI 폴링보다 정확)
RESTART_MS="$(grep -h '^restart_ms=' "$OUT/meta.txt" 2>/dev/null | cut -d= -f2)"
FAIL_MS="$(grep -h '^fail_ms=' "$OUT/meta.txt" 2>/dev/null | cut -d= -f2)"
if [ -n "$RESTART_MS" ] && [ -n "$FAIL_MS" ]; then
  ch "
  SELECT toUnixTimestamp64Milli(min(ts))                    AS resume_ms,
         resume_ms - ${RESTART_MS}                          AS restart_to_resume_ms,
         resume_ms - ${FAIL_MS}                             AS fail_to_resume_ms
  FROM e2.msgs FINAL
  WHERE ts > fromUnixTimestamp64Milli(toInt64(${RESTART_MS}), 'UTC')
  FORMAT Vertical" | tee -a "$OUT/dup-summary.txt"
fi

ch "DROP TABLE e2.msgs;"
log "done → $OUT/dup-summary.txt, dup-groups.tsv"
