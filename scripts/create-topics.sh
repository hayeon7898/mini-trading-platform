#!/usr/bin/env bash
# Kafka 토픽 생성 (auto.create.topics.enable=false 이므로 커넥터 등록 전에 실행)
#
# 사용법:
#   ./scripts/create-topics.sh                 # 기본 파티션 6
#   PARTITIONS=12 ./scripts/create-topics.sh   # 실험 E4: 파티션 수 비교
#
# 이미 있는 토픽은 건너뜀(--if-not-exists). 파티션 수를 바꾸려면 토픽을 지우고 다시 실행.
set -euo pipefail

# Git Bash(Windows)가 /opt/... 경로를 C:/Program Files/Git/opt/... 로 바꾸는 것을 방지
export MSYS_NO_PATHCONV=1

BROKER_CONTAINER="${BROKER_CONTAINER:-mts-kafka-1}"
PARTITIONS="${PARTITIONS:-6}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
KAFKA_BIN=/opt/kafka/bin

topics() {
  docker exec "$BROKER_CONTAINER" "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server localhost:9092 "$@"
}

create() {
  local name="$1"; shift
  topics --create --if-not-exists --topic "$name" "$@"
  echo "✔ $name"
}

echo "== 데이터 토픽 (파티션 $PARTITIONS, RF $REPLICATION_FACTOR) =="
# Debezium 기본 토픽 이름: <topic.prefix>.<database>.<table>
for t in mts.trading.orders mts.trading.executions; do
  create "$t" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR"
done

echo "== Debezium 스키마 히스토리 토픽 =="
# DDL 이력을 순서대로 보관해야 하므로 파티션 1개, 무기한 보존 (Debezium 요구사항)
create schema-history.mts --partitions 1 --replication-factor "$REPLICATION_FACTOR" \
  --config retention.ms=-1 --config retention.bytes=-1 --config cleanup.policy=delete

echo
echo "== 확인 =="
for t in mts.trading.orders mts.trading.executions schema-history.mts; do
  topics --describe --topic "$t" | head -1
done
