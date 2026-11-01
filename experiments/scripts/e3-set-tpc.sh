#!/usr/bin/env bash
# E3 적재 방식 전환 — 운영 큐 두 개의 kafka_thread_per_consumer를 바꾼다 (#43)
#
# Kafka 엔진은 ALTER ... MODIFY SETTING을 지원하지 않아(ClickHouse 26.8, NOT_IMPLEMENTED) 큐를 다시 만든다.
# 지금 떠 있는 정의(SHOW CREATE)를 읽어 설정만 바꾸므로, DDL은 init SQL 한 곳에만 둔다.
#   순서: MV 삭제 → 큐 삭제 → 새 설정으로 큐 생성 → MV 재생성 (MV가 생기는 순간 소비 재개)
# 컨슈머 그룹 이름이 같으므로 Kafka에 커밋된 오프셋부터 이어서 읽는다. 적재된 데이터는 MV의 TO 대상 테이블에 그대로 남는다.
#
# 사용법: bash experiments/scripts/e3-set-tpc.sh <0|1>
#   실험 후 원래 설정으로 되돌리기: bash experiments/scripts/e3-set-tpc.sh 0
set -euo pipefail

if [ "$#" -ne 1 ] || [[ ! "$1" =~ ^[01]$ ]]; then
  echo "usage: $0 <0|1>" >&2
  exit 1
fi
TPC="$1"

ch() { docker exec -i mts-clickhouse clickhouse-client --multiquery "$@"; }

for pair in "kafka_executions_queue:executions_mv" "kafka_orders_queue:orders_mv"; do
  queue="${pair%%:*}"
  mv="${pair##*:}"

  queue_ddl="$(ch --format TSVRaw -q "SHOW CREATE TABLE mts.${queue}")"
  mv_ddl="$(ch --format TSVRaw -q "SHOW CREATE TABLE mts.${mv}")"

  # 기존 값이 있으면 지우고 SETTINGS 끝에 새 값을 붙인다 (Kafka 엔진 DDL은 SETTINGS가 마지막 절)
  new_queue_ddl="$(printf '%s' "$queue_ddl" \
    | sed -E 's/,[[:space:]]*kafka_thread_per_consumer[[:space:]]*=[[:space:]]*[0-9]+//')"
  new_queue_ddl="${new_queue_ddl}, kafka_thread_per_consumer = ${TPC}"

  printf 'DROP TABLE mts.%s;\nDROP TABLE mts.%s;\n%s;\n%s;\n' \
    "$mv" "$queue" "$new_queue_ddl" "$mv_ddl" | ch

  echo "[$queue] $(ch -q "SELECT extract(engine_full, 'kafka_thread_per_consumer = [0-9]+') \
    FROM system.tables WHERE database = 'mts' AND name = '${queue}'")"
done
