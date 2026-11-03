#!/usr/bin/env bash
# 두 데이터 토픽의 파티션별 끝 오프셋을 파일로 남긴다 (07 스펙 E5).
# 출력 형식은 kafka-get-offsets.sh 그대로(topic:partition:offset)라 key-order의 --boundary로도 쓸 수 있다.
#
# 사용법:
#   bash experiments/scripts/e5-offsets.sh <출력 파일>
#
# 부하 전후로 한 번씩 남기고 두 파일의 차이로 파티션별 유입량을 구한다.
set -euo pipefail

# Git Bash(Windows)가 /opt/... 경로를 바꾸는 것을 방지
export MSYS_NO_PATHCONV=1

OUT_FILE="${1:?출력 파일 경로가 필요함}"
BROKER_CONTAINER="${BROKER_CONTAINER:-mts-kafka-1}"

mkdir -p "$(dirname "$OUT_FILE")"
docker exec "$BROKER_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic-partitions 'mts\.trading\.(orders|executions)' \
  | sort > "$OUT_FILE"

echo "[offsets] $(date '+%F %T') → $OUT_FILE"
cat "$OUT_FILE"
