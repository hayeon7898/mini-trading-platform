#!/usr/bin/env bash
# K2 깨진 메시지 주입 (#65, 07 스펙). 장애 재현 실험용이다.
#
# 사용법: bash experiments/scripts/k2-inject.sh <malformed|type> <결과 폴더>
#   malformed : JSON 문법 오류 (닫는 중괄호 없음)
#   type      : JSON은 맞지만 quantity가 문자열 (컬럼 타입 변경 같은 상황)
#
# 키는 {"account_id":1}로 고정해 항상 같은 파티션에 들어가게 한다.
# id는 실제 체결과 겹치지 않는 표식 값(malformed 900000001, type 900000002)이다.
# 주입 직전 끝 오프셋부터 파티션마다 읽어 표식 id를 찾고, 파티션과 오프셋을 <결과 폴더>/marker.txt에 남긴다.
set -euo pipefail

export MSYS_NO_PATHCONV=1

CASE="${1:?malformed 또는 type이 필요함}"
OUT="${2:?결과 폴더가 필요함}"
TOPIC=mts.trading.executions
BROKER=mts-kafka-1
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

now_ms="$(date +%s%3N)"
case "$CASE" in
  malformed)
    marker=900000001
    value="{\"id\":${marker},\"order_id\":1,\"account_id\":1,\"symbol\":\"K2TEST\",\"side\":\"BUY\",\"price\":\"1.0000\",\"quantity\":1,\"executed_at\":${now_ms}"
    ;;
  type)
    marker=900000002
    value="{\"id\":${marker},\"order_id\":1,\"account_id\":1,\"symbol\":\"K2TEST\",\"side\":\"BUY\",\"price\":\"1.0000\",\"quantity\":\"abc\",\"executed_at\":${now_ms},\"__op\":\"c\",\"__source_ts_ms\":${now_ms}}"
    ;;
  *) echo "malformed 또는 type이어야 함: $CASE" >&2; exit 1 ;;
esac

mkdir -p "$OUT"
before="$OUT/offsets-before-inject.txt"
bash "$ROOT/experiments/scripts/e5-offsets.sh" "$before" > /dev/null

printf '%s|%s\n' '{"account_id":1}' "$value" | docker exec -i "$BROKER" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --reader-property parse.key=true --reader-property 'key.separator=|' 2>/dev/null
inject_ms="$(date +%s%3N)"

# 표식 메시지 위치 찾기 (주입 직전 오프셋부터 파티션마다 최대 5000건)
found=""
while IFS=: read -r _ p off; do
  hit="$(docker exec "$BROKER" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC" --partition "$p" --offset "$off" \
    --max-messages 5000 --timeout-ms 5000 \
    --formatter-property print.partition=true --formatter-property print.offset=true 2>/dev/null \
    | grep "\"id\":${marker}," | head -n 1 || true)"
  if [ -n "$hit" ]; then
    found="$(printf '%s' "$hit" | sed -E 's/^Partition:([0-9]+)[[:space:]]+Offset:([0-9]+).*/\1 \2/')"
    break
  fi
done < <(grep "^${TOPIC}:" "$before")
[ -n "$found" ] || { echo "표식 메시지를 찾지 못함 (id=$marker)" >&2; exit 1; }

read -r m_part m_off <<< "$found"
{
  echo "case=$CASE"
  echo "marker_id=$marker"
  echo "inject_ms=$inject_ms"
  echo "partition=$m_part"
  echo "offset=$m_off"
} | tee "$OUT/marker.txt"