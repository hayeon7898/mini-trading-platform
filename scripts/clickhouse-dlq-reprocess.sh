#!/usr/bin/env bash
# ClickHouse dead letter queue 재처리 (#66)
#
# Kafka 큐는 kafka_handle_error_mode = dead_letter_queue라, 파싱하지 못한 메시지는
# system.dead_letter_queue에 원본, 키, 위치와 함께 남고 대상 테이블에는 들어가지 않는다.
# 이 스크립트로 원본을 파일로 꺼내 고친 뒤, 같은 키로 원래 토픽에 다시 넣는다.
#
# 사용법:
#   bash scripts/clickhouse-dlq-reprocess.sh list   [분(60)]
#   bash scripts/clickhouse-dlq-reprocess.sh export <파일> [분(60)] [큐 테이블]
#   bash scripts/clickhouse-dlq-reprocess.sh send   <파일>
#
# 절차:
#   1. list로 최근 DLQ 기록을 본다.
#   2. export로 고칠 메시지를 파일로 꺼낸다. 한 줄이 메시지 하나다 (탭 구분).
#   3. 파일의 raw 열(마지막 열)만 고친다. 고치지 않을 줄은 지운다.
#   4. send로 다시 넣는다. 보내기 전에 ClickHouse가 큐와 같은 구조로 파싱해 보고, 하나라도 실패하면 아무것도 보내지 않는다.
#
# - 같은 키로 보내므로 원래 파티션으로 간다 (K1의 계좌 단위 파티션 전제 유지).
# - 다시 넣은 메시지는 파티션 끝에 붙어 같은 키의 이후 이벤트보다 뒤에 놓인다.
#   체결은 ReplacingMergeTree(id), 주문은 version 기준이라 도착 순서가 결과를 바꾸지 않는다.
# - 같은 파일을 두 번 보내도 같은 이유로 결과는 같다. DLQ 기록은 지우지 않는다 (30일 보관 후 TTL로 삭제).
set -euo pipefail

export MSYS_NO_PATHCONV=1

CH_CONTAINER="${CLICKHOUSE_CONTAINER:-mts-clickhouse}"
BROKER="${BROKER_CONTAINER:-mts-kafka-1}"
TAB=$'\t'

# shellcheck disable=SC2016  # 비밀번호 변수는 컨테이너 안에서 풀려야 한다
ch() { docker exec -i "$CH_CONTAINER" sh -c 'clickhouse-client --user "${CLICKHOUSE_USER:-default}" --password "$CLICKHOUSE_PASSWORD" "$@"' sh "$@"; }

cmd="${1:-}"
case "$cmd" in
  list)
    minutes="${2:-60}"
    ch -q "SYSTEM FLUSH LOGS" > /dev/null
    ch --format PrettyCompactMonoBlock -q "
      SELECT event_time, table, kafka_partition AS p, kafka_offset AS offset, kafka_key AS key,
             substring(error, 1, 60) AS error, substring(raw_message, 1, 60) AS raw
      FROM system.dead_letter_queue
      WHERE database = 'mts' AND event_time >= now() - INTERVAL ${minutes} MINUTE
      ORDER BY event_time"
    ;;

  export)
    file="${2:?파일 경로가 필요함}"
    minutes="${3:-60}"
    table="${4:-}"
    [ ! -e "$file" ] || { echo "이미 있는 파일: $file" >&2; exit 1; }
    filter=""
    [ -z "$table" ] || filter="AND table = '${table}'"
    ch -q "SYSTEM FLUSH LOGS" > /dev/null
    {
      echo "# 재처리 파일. 열: table, topic, partition, offset, key, raw (탭 구분)"
      echo "# raw 열만 고치고, 다시 넣지 않을 줄은 지운다. #으로 시작하는 줄은 무시한다."
      # 같은 위치가 여러 번 기록됐을 수 있어 위치마다 하나만 꺼낸다
      ch --format TSVRaw -q "
        SELECT table, kafka_topic_name, kafka_partition, kafka_offset, kafka_key, any(raw_message)
        FROM system.dead_letter_queue
        WHERE database = 'mts' AND event_time >= now() - INTERVAL ${minutes} MINUTE ${filter}
        GROUP BY table, kafka_topic_name, kafka_partition, kafka_offset, kafka_key
        ORDER BY kafka_topic_name, kafka_partition, kafka_offset"
    } > "$file"
    echo "$(grep -vc '^#' "$file")건을 $file에 꺼냈다"
    ;;

  send)
    file="${2:?파일 경로가 필요함}"
    [ -f "$file" ] || { echo "파일 없음: $file" >&2; exit 1; }

    # 1) 모두 파싱되는지 먼저 확인한다 (다시 DLQ로 빠지는 일을 막기 위해)
    failed=0
    while IFS="$TAB" read -r table topic part off _key raw; do
      [ -n "$table" ] && [ "${table:0:1}" != "#" ] || continue
      structure="$(ch -q "SELECT arrayStringConcat(groupArray(concat(name, ' ', type)), ', ')
        FROM (SELECT name, type FROM system.columns
              WHERE database = 'mts' AND table = '${table}' ORDER BY position)" < /dev/null)"
      # ch는 docker exec -i라 표준 입력을 막아 두지 않으면 while이 읽는 파일 줄을 가져가 버린다
      if ! ch --param_raw="$raw" -q "SELECT count() FROM format(JSONEachRow, '${structure}', {raw:String})" < /dev/null > /dev/null 2>&1; then
        echo "파싱 실패: ${topic} p${part} offset ${off}" >&2
        failed=$(( failed + 1 ))
      fi
    done < "$file"
    [ "$failed" -eq 0 ] || { echo "${failed}건이 아직 파싱되지 않아 아무것도 보내지 않았다" >&2; exit 1; }

    # 2) 토픽마다 같은 키로 보낸다
    sent=0
    while read -r topic; do
      n="$(grep -v '^#' "$file" | awk -F"$TAB" -v t="$topic" '$2 == t' | wc -l)"
      grep -v '^#' "$file" | awk -F"$TAB" -v t="$topic" 'BEGIN {OFS = "\t"} $2 == t {print $5, $6}' \
        | docker exec -i "$BROKER" /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server localhost:9092 --topic "$topic" \
            --reader-property parse.key=true --reader-property "key.separator=${TAB}" 2>/dev/null
      echo "[$topic] ${n}건 전송"
      sent=$(( sent + n ))
    done < <(grep -v '^#' "$file" | cut -f2 | sort -u)
    echo "총 ${sent}건을 다시 넣었다"
    ;;

  *)
    echo "usage: $0 list [분] | export <파일> [분] [큐 테이블] | send <파일>" >&2
    exit 1
    ;;
esac
