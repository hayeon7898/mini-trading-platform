#!/usr/bin/env bash
# E1 브로커 장애와 acks (장애 재현 실험) — 1회 실행
#
# 토픽 생성(파티션 1, RF 3, 리더는 활성 컨트롤러가 아닌 브로커) → 일련번호 전송 시작
# → KILL_AT초 뒤 리더 docker kill → 리더 변경 감지 → 전송 종료 → 브로커 재기동·URP 0 대기 → 검증
#
# 사용법:
#   bash experiments/scripts/e1-run.sh <acks:1|all> <min_isr:1|2> <idempotence:true|false> <run>
# 환경변수(선택):
#   RATE=5000 DURATION=60 KILL_AT=20
#   TAG=rate20k                  조합 이름 뒤에 붙는 태그 (같은 조합을 다른 조건으로 돌릴 때)
#   PRODUCER_CONFIG=k=v,k=v      프로듀서 추가 설정 (예: socket.connection.setup.timeout.ms=1000)
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <acks:1|all> <min_isr:1|2> <idempotence:true|false> <run>" >&2
  exit 1
fi
ACKS="$1"
MIN_ISR="$2"
IDEM="$3"
RUN="$4"
RATE="${RATE:-5000}"
DURATION="${DURATION:-60}"
KILL_AT="${KILL_AT:-20}"
TAG="${TAG:-}"
PRODUCER_CONFIG="${PRODUCER_CONFIG:-}"

if [ "$ACKS" = "1" ] && [ "$IDEM" = "true" ]; then
  echo "acks=1 과 idempotence=true 는 함께 쓸 수 없음 (멱등성은 acks=all 필요)" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULT_DIR="$ROOT/experiments/results/20261021-E1-broker-failure-acks"   # 날짜가 바뀌어도 경로 고정
IDEM_TAG="idemoff"
[ "$IDEM" = "true" ] && IDEM_TAG="idemon"
COMBO="acks${ACKS}-isr${MIN_ISR}-${IDEM_TAG}${TAG:+-$TAG}"
TOPIC="e1.${COMBO}.r${RUN}"
OUT="$RESULT_DIR/raw/${COMBO}-r${RUN}"
TOOL="$ROOT/experiments/producer-tool/build/install/producer-tool"

if [ -e "$OUT" ]; then
  echo "이미 결과가 있음: $OUT (회차 번호를 바꾸거나 폴더를 지울 것)" >&2
  exit 1
fi
if [ ! -x "$TOOL/bin/producer-tool" ]; then
  echo "producer-tool 빌드 필요: (cd experiments/producer-tool && ./gradlew installDist)" >&2
  exit 1
fi
mkdir -p "$OUT"

NET="$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"

now_ms() { date +%s%3N; }
log() { echo "[$(date -u +%H:%M:%S.%3N)] $*" | tee -a "$OUT/run.log"; }

# 살아 있는 브로커 하나를 골라 Kafka CLI 실행
kafka_cli() {
  local via="$1"
  shift
  local tool="$1"
  shift
  docker exec "mts-kafka-${via}" "/opt/kafka/bin/${tool}" --bootstrap-server "kafka-${via}:9092" "$@"
}

leader_of() {
  kafka_cli "$1" kafka-topics.sh --describe --topic "$TOPIC" 2>/dev/null |
    awk '/Partition: 0/ {for (i = 1; i <= NF; i++) if ($i == "Leader:") print $(i + 1)}'
}

run_tool() {
  docker run --rm --network "$NET" -v "$TOOL:/app:ro" -v "$OUT:/out" \
    eclipse-temurin:21.0.12.1_1-jre-noble /app/bin/producer-tool "$@"
}

# --- 0. 사전 조건: 브로커 3대 실행 중, URP 0 ----------------------------------
for b in 1 2 3; do
  if [ "$(docker inspect -f '{{.State.Running}}' "mts-kafka-$b")" != "true" ]; then
    echo "mts-kafka-$b 가 실행 중이 아님" >&2
    exit 1
  fi
done
URP_BEFORE="$(kafka_cli 1 kafka-topics.sh --describe --under-replicated-partitions | grep -c 'Partition:' || true)"
if [ "$URP_BEFORE" != "0" ]; then
  echo "URP $URP_BEFORE 개 — 복구가 끝난 뒤 실행할 것" >&2
  exit 1
fi

# --- 1. 리더 배치: 활성 컨트롤러가 아닌 브로커 ---------------------------------
CONTROLLER="$(kafka_cli 1 kafka-metadata-quorum.sh describe --status | awk '/LeaderId:/ {print $2}')"
LEADER=3
[ "$CONTROLLER" = "3" ] && LEADER=1
FOLLOWERS=()
for b in 1 2 3; do
  [ "$b" != "$LEADER" ] && FOLLOWERS+=("$b")
done
VIA="${FOLLOWERS[0]}"   # 리더를 죽인 뒤에도 쓸 CLI 접속 브로커

log "combo=$COMBO run=$RUN topic=$TOPIC rate=$RATE duration=$DURATION kill_at=$KILL_AT"
log "active_controller=$CONTROLLER leader=$LEADER followers=${FOLLOWERS[*]}"

kafka_cli "$VIA" kafka-topics.sh --create --topic "$TOPIC" \
  --replica-assignment "${LEADER}:${FOLLOWERS[0]}:${FOLLOWERS[1]}" \
  --config "min.insync.replicas=${MIN_ISR}" >/dev/null
for _ in $(seq 1 30); do
  [ "$(leader_of "$VIA")" = "$LEADER" ] && break
  sleep 1
done
kafka_cli "$VIA" kafka-topics.sh --describe --topic "$TOPIC" | tee "$OUT/describe-before.txt"

# --- 2. 전송 시작 -------------------------------------------------------------
PRODUCE_ARGS=(--topic "$TOPIC" --acks "$ACKS" --idempotence "$IDEM" --rate "$RATE" --duration "$DURATION")
if [ -n "$PRODUCER_CONFIG" ]; then
  PRODUCE_ARGS+=(--producer-config "$PRODUCER_CONFIG")
  log "producer_config=$PRODUCER_CONFIG"
fi
START_MS="$(now_ms)"
run_tool produce "${PRODUCE_ARGS[@]}" >"$OUT/producer.log" 2>&1 &
PRODUCER_PID=$!

# --- 3. 리더 강제 종료 --------------------------------------------------------
sleep "$KILL_AT"
KILL_MS="$(now_ms)"
docker kill "mts-kafka-${LEADER}" >/dev/null
log "killed mts-kafka-${LEADER} at kill_ms=$KILL_MS"

# --- 4. 리더 변경 감지 (CLI 호출당 1~2초라 해상도는 거침, 주 지표는 프로듀서 ack 공백) --
NEW_LEADER=""
DETECT_MS=""
for _ in $(seq 1 60); do
  l="$(leader_of "$VIA" || true)"
  if [ -n "$l" ] && [ "$l" != "$LEADER" ] && [ "$l" != "none" ] && [ "$l" != "-1" ]; then
    NEW_LEADER="$l"
    DETECT_MS="$(now_ms)"
    break
  fi
  sleep 0.5
done
if [ -n "$NEW_LEADER" ]; then
  log "new_leader=$NEW_LEADER detected_after_ms=$((DETECT_MS - KILL_MS))"
else
  log "new_leader=none (60회 조회 동안 리더 없음)"
fi
kafka_cli "$VIA" kafka-topics.sh --describe --topic "$TOPIC" | tee "$OUT/describe-during.txt"

# --- 5. 전송 종료 대기 ---------------------------------------------------------
wait "$PRODUCER_PID" || log "producer exited with $?"
END_MS="$(now_ms)"
if [ $((END_MS - START_MS)) -gt $(((DURATION + 180) * 1000)) ]; then
  log "INVALID: 실행 시간이 전송 시간보다 3분 이상 김 (절전 등 시계 점프 의심)"
fi

# --- 6. 브로커 복구, 이 토픽 URP 0 대기 -------------------------------------------
docker start "mts-kafka-${LEADER}" >/dev/null
log "restarted mts-kafka-${LEADER}"
RECOVERED=""
for _ in $(seq 1 90); do
  if kafka_cli "$VIA" kafka-topics.sh --describe --topic "$TOPIC" 2>/dev/null |
    awk '/Partition: 0/ {for (i = 1; i <= NF; i++) if ($i == "Isr:") print $(i + 1)}' |
    grep -Eq '^[0-9]+,[0-9]+,[0-9]+$'; then
    RECOVERED="$(now_ms)"
    break
  fi
  sleep 2
done
if [ -n "$RECOVERED" ]; then
  log "isr_restored_after_restart_ms=$((RECOVERED - END_MS))"
else
  log "ISR 복구 확인 실패 (3분)"
fi
kafka_cli "$VIA" kafka-topics.sh --describe --topic "$TOPIC" | tee "$OUT/describe-after.txt"

# --- 7. 검증 ------------------------------------------------------------------
run_tool verify --topic "$TOPIC" >"$OUT/verify.log" 2>&1
cat "$OUT/verify.log"

# --- 8. 부가 기록: 브로커 로그(이 토픽), Prometheus ISR 추이 ------------------------
SINCE=$((KILL_MS / 1000 - 5))
for b in 1 2 3; do
  docker logs --since "$SINCE" "mts-kafka-$b" 2>&1 | grep -F "$TOPIC" >"$OUT/broker-$b.log" || true
done
bash "$ROOT/experiments/scripts/prom-range.sh" \
  "kafka_topic_partition_in_sync_replica{topic=\"$TOPIC\"}" \
  $((START_MS / 1000 - 10)) $((END_MS / 1000 + 10)) 1s >"$OUT/isr.csv" 2>/dev/null || true

{
  echo "combo=$COMBO"
  echo "run=$RUN"
  echo "topic=$TOPIC"
  echo "acks=$ACKS"
  echo "min_isr=$MIN_ISR"
  echo "idempotence=$IDEM"
  echo "rate=$RATE"
  echo "duration=$DURATION"
  echo "kill_at=$KILL_AT"
  echo "tag=$TAG"
  echo "producer_config=$PRODUCER_CONFIG"
  echo "active_controller=$CONTROLLER"
  echo "leader=$LEADER"
  echo "new_leader=${NEW_LEADER:-none}"
  echo "start_ms=$START_MS"
  echo "kill_ms=$KILL_MS"
  echo "leader_detected_ms=${DETECT_MS:-}"
  echo "end_ms=$END_MS"
} >"$OUT/meta.txt"

log "done → $OUT"