#!/usr/bin/env bash
# E4 파티션 수와 처리량 — 실험 토픽에 체결 이벤트를 미리 채운다 (토픽당 1회)
#
# 사용법: bash experiments/scripts/e4-prefill.sh <partitions>
# 환경변수(선택): N=3000000 RATE=100000
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <partitions>" >&2
  exit 1
fi
P="$1"
N="${N:-3000000}"
RATE="${RATE:-100000}"
DURATION=$(((N + RATE - 1) / RATE))
N=$((RATE * DURATION))

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULT_DIR="$ROOT/experiments/results/20261022-E4-partition-throughput"   # 날짜가 바뀌어도 경로 고정
TOPIC="e4.p${P}"
OUT="$RESULT_DIR/raw/prefill-p${P}"
TOOL="$ROOT/experiments/producer-tool/build/install/producer-tool"
NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
KT=(docker exec mts-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092)

if [ -e "$OUT" ]; then
  echo "이미 채웠음: $OUT" >&2
  exit 1
fi
mkdir -p "$OUT"

"${KT[@]}" --create --topic "$TOPIC" --partitions "$P" --replication-factor 3 >/dev/null
"${KT[@]}" --describe --topic "$TOPIC" | tee "$OUT/describe.txt"

# 처리량용 배치 설정 + lz4 압축(브로커 디스크 절약, 운영에서도 흔한 설정)
docker run --rm --network "$NET" -v "$TOOL:/app:ro" -v "$OUT:/out" eclipse-temurin:21.0.12.1_1-jre-noble \
  /app/bin/producer-tool produce --topic "$TOPIC" --format execution --key id --record-each false \
  --acks all --idempotence true --rate "$RATE" --duration "$DURATION" \
  --producer-config linger.ms=20,batch.size=262144,compression.type=lz4 2>&1 | tee "$OUT/producer.log"

# 이전 빌드(체결 JSON 형식 이전)로 실행되면 인자가 무시되고 일련번호 문자열이 들어감 → 즉시 실패시킴
if ! grep -q 'format=execution key=id' "$OUT/producer.log"; then
  echo "producer-tool이 체결 형식을 지원하지 않는 이전 빌드임. 재빌드 후 토픽을 지우고 다시 실행" >&2
  exit 1
fi

docker exec mts-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9092 \
  --topic "$TOPIC" | tee "$OUT/offsets.txt"
TOTAL="$(awk -F: '{s += $3} END {print s}' "$OUT/offsets.txt")"
echo "expected=$N actual=$TOTAL" | tee "$OUT/check.txt"
if [ "$TOTAL" != "$N" ]; then
  echo "건수 불일치" >&2
  exit 1
fi