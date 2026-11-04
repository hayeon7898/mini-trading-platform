#!/usr/bin/env bash
# K2 체결 큐 오류 처리 모드 전환 (#65, 07 스펙)
#
# 사용법: bash experiments/scripts/k2-set-error-mode.sh <모드>
#   default           : kafka_handle_error_mode 없음 (깨진 메시지에서 예외, 같은 위치 재시도)
#   stream            : stream 모드, 기존 MV 그대로 (_error를 거르지 않음)
#   stream-filtered   : stream 모드, 기존 MV에 _error 거름 조건 + 오류 메시지를 k2_execution_errors에 저장
#   dead_letter_queue : DLQ 모드, 기존 MV 그대로 (깨진 메시지는 system.dead_letter_queue로)
#   restore           : default로 되돌리고 실험용 오류 테이블과 실험 표식 행을 지운다
#
# Kafka 엔진은 MODIFY SETTING을 지원하지 않아 e3-set-tpc.sh와 같이 큐와 MV를 다시 만든다.
# 지금 떠 있는 정의(SHOW CREATE)를 읽어 설정과 거름 조건만 바꾸므로, DDL 원본은 init SQL 한 곳에 둔다.
# 컨슈머 그룹 이름이 같아 커밋된 오프셋부터 이어서 읽는다.
#
# MV를 만드는 순간 소비가 시작되므로 본 MV를 먼저 만들고 오류 MV를 바로 뒤에 만든다.
# 그 사이(1초 미만)에 들어온 깨진 메시지는 오류 테이블에 남지 않을 수 있지만, 주입은 모드 전환 뒤 한참 뒤에 한다.
set -euo pipefail

MODE="${1:?모드가 필요함: default|stream|stream-filtered|dead_letter_queue|restore}"
case "$MODE" in
  default|stream|stream-filtered|dead_letter_queue|restore) ;;
  *) echo "알 수 없는 모드: $MODE" >&2; exit 1 ;;
esac

QUEUE=kafka_executions_queue
MV=executions_mv
ERR_TABLE=k2_execution_errors
ERR_MV=k2_execution_errors_mv
FILTER='WHERE length(_error) = 0'
MARKER_MIN_ID=900000000   # k2-inject.sh가 넣는 표식 메시지의 id (실제 체결 id보다 훨씬 큼)

# shellcheck disable=SC2016  # 비밀번호 변수는 컨테이너 안에서 풀려야 한다
ch() { docker exec -i mts-clickhouse sh -c 'clickhouse-client --user "${CLICKHOUSE_USER:-default}" --password "$CLICKHOUSE_PASSWORD" --multiquery "$@"' sh "$@"; }

queue_ddl="$(ch --format TSVRaw -q "SHOW CREATE TABLE mts.${QUEUE}")"
mv_ddl="$(ch --format TSVRaw -q "SHOW CREATE TABLE mts.${MV}")"

# 설정과 거름 조건을 지운 기준 DDL (어떤 모드에서 시작해도 같은 결과가 되게)
base_queue_ddl="$(printf '%s' "$queue_ddl" \
  | sed -E "s/,[[:space:]]*kafka_handle_error_mode[[:space:]]*=[[:space:]]*'[^']*'//")"
base_mv_ddl="$(printf '%s' "$mv_ddl" \
  | sed -E 's/[[:space:]]*WHERE[[:space:]]+length\(_error\)[[:space:]]*=[[:space:]]*0[[:space:]]*$//')"
# SHOW CREATE는 WHERE를 줄 맨 앞에 두므로 앞 공백이 없어도 지운다

new_queue_ddl="$base_queue_ddl"
new_mv_ddl="$base_mv_ddl"
case "$MODE" in
  stream)            new_queue_ddl="${base_queue_ddl}, kafka_handle_error_mode = 'stream'" ;;
  stream-filtered)   new_queue_ddl="${base_queue_ddl}, kafka_handle_error_mode = 'stream'"
                     new_mv_ddl="${base_mv_ddl} ${FILTER}" ;;
  dead_letter_queue) new_queue_ddl="${base_queue_ddl}, kafka_handle_error_mode = 'dead_letter_queue'" ;;
esac

err_ddl=""
if [ "$MODE" = "stream-filtered" ]; then
  err_ddl="CREATE TABLE IF NOT EXISTS mts.${ERR_TABLE}
(
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),
    topic       String,
    partition   UInt64,
    offset      UInt64,
    raw_message String,
    error       String
)
ENGINE = MergeTree ORDER BY received_at;
CREATE MATERIALIZED VIEW mts.${ERR_MV} TO mts.${ERR_TABLE} AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset, _raw_message AS raw_message, _error AS error
FROM mts.${QUEUE} WHERE length(_error) > 0;"
fi

printf 'DROP TABLE IF EXISTS mts.%s;\nDROP TABLE mts.%s;\nDROP TABLE mts.%s;\n%s;\n%s;\n%s\n' \
  "$ERR_MV" "$MV" "$QUEUE" "$new_queue_ddl" "$new_mv_ddl" "$err_ddl" | ch

if [ "$MODE" = "restore" ]; then
  ch -q "DROP TABLE IF EXISTS mts.${ERR_TABLE}"
  # stream 모드에서 거름 조건 없이 들어간 행 (기본값 id = 0 또는 표식 id)을 지운다
  ch -q "ALTER TABLE mts.executions DELETE WHERE id = 0 OR id >= ${MARKER_MIN_ID} SETTINGS mutations_sync = 1"
fi

echo "[$QUEUE] $(ch -q "SELECT ifNull(nullIf(extract(engine_full, 'kafka_handle_error_mode = ''[^'']+'''), ''), 'default')
  FROM system.tables WHERE database = 'mts' AND name = '${QUEUE}'")"
echo "[$MV] $(ch -q "SELECT if(position(create_table_query, 'length(_error) = 0') > 0, 'filtered', 'no filter')
  FROM system.tables WHERE database = 'mts' AND name = '${MV}'")"
echo "[$ERR_MV] $(ch -q "EXISTS TABLE mts.${ERR_MV}")"