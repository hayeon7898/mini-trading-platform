#!/usr/bin/env bash
# Debezium 커넥터의 메시지 키를 계좌와 종목 사이에서 전환한다 (07 스펙 E5).
#
# 사용법:
#   bash experiments/scripts/e5-switch-key.sh account|symbol <결과 폴더>
#
# 하는 일:
#   1. 처리 대기 주문(ACCEPTED, PARTIAL)이 남아 있으면 멈춘다.
#   2. 전환 직전 끝 오프셋을 <결과 폴더>/boundary-to-<키>-<시각>.txt로 남긴다.
#   3. infra/connect/mts-trading-connector.json에서 message.key.columns 한 줄만 바꿔 PUT으로 보낸다.
#   4. 태스크가 RUNNING이 될 때까지 기다리고, 적용된 키 설정을 출력한다.
#
# - 설정 원본은 운영 JSON 하나만 쓴다. 종목 키 JSON을 infra/connect에 두면
#   register.sh가 두 번째 커넥터로 등록해 초기 스냅샷이 돈다 (#61).
# - 커넥터 이름이 같아 기존 오프셋을 이어받으므로 스냅샷이 일어나지 않는다.
# - 호스트의 :8083 접속 문제를 피하려고 Connect 컨테이너 안에서 curl을 호출한다.
set -euo pipefail

export MSYS_NO_PATHCONV=1

KEY="${1:?account 또는 symbol이 필요함}"
OUT_DIR="${2:?결과 폴더가 필요함}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CONFIG="$ROOT/infra/connect/mts-trading-connector.json"
NAME="$(basename "$CONFIG" .json)"
CONNECT_CONTAINER="${CONNECT_CONTAINER:-mts-connect}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-mts-mysql}"
CONNECT_URL="http://localhost:8083/connectors/$NAME"

case "$KEY" in
  account) COLUMN=account_id ;;
  symbol)  COLUMN=symbol ;;
  *) echo "키는 account 또는 symbol이어야 함: $KEY" >&2; exit 1 ;;
esac
KEY_COLUMNS="trading.orders:$COLUMN;trading.executions:$COLUMN"

# 1. 처리 대기 주문 확인
# shellcheck disable=SC2016  # 비밀번호 변수는 컨테이너 안에서 풀려야 한다
pending=$(docker exec "$MYSQL_CONTAINER" sh -c \
  'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "SELECT COUNT(*) FROM trading.orders WHERE status IN (\"ACCEPTED\",\"PARTIAL\")"' \
  2>/dev/null)
if [[ "$pending" != "0" ]]; then
  echo "처리 대기 주문이 남아 있음 ($pending건). 0이 된 뒤 다시 실행" >&2
  exit 1
fi

# 2. 전환 직전 오프셋
boundary="$OUT_DIR/boundary-to-$KEY-$(date +%Y%m%d-%H%M%S).txt"
bash "$ROOT/experiments/scripts/e5-offsets.sh" "$boundary"

# 3. 키 한 줄만 바꾼 설정 전송
new_config=$(sed -E \
  "s/(\"message\.key\.columns\"[[:space:]]*:[[:space:]]*\")[^\"]*\"/\1$KEY_COLUMNS\"/" \
  "$CONFIG")
if ! grep -q "\"message.key.columns\": *\"$KEY_COLUMNS\"" <<< "$new_config"; then
  echo "설정에서 message.key.columns를 바꾸지 못함: $CONFIG" >&2
  exit 1
fi

code=$(printf '%s' "$new_config" | docker exec -i "$CONNECT_CONTAINER" \
  curl -s -o /dev/null -w '%{http_code}' -X PUT \
  -H 'Content-Type: application/json' --data @- "$CONNECT_URL/config")
if [[ "$code" -ge 400 ]]; then
  echo "[$NAME] 설정 전송 실패 (HTTP $code)" >&2
  exit 1
fi
echo "[$NAME] 설정 전송 (HTTP $code)"

# 4. 태스크 RUNNING 대기
for _ in $(seq 1 30); do
  status=$(docker exec "$CONNECT_CONTAINER" curl -s "$CONNECT_URL/status")
  if grep -q '"tasks":\[{"id":0,"state":"RUNNING"' <<< "$status"; then
    break
  fi
  sleep 2
done
if ! grep -q '"tasks":\[{"id":0,"state":"RUNNING"' <<< "$status"; then
  echo "[$NAME] 60초 안에 태스크가 RUNNING이 되지 않음" >&2
  echo "$status" >&2
  exit 1
fi

applied=$(docker exec "$CONNECT_CONTAINER" curl -s "$CONNECT_URL/config" \
  | grep -o '"message.key.columns":"[^"]*"')
echo "[$NAME] 태스크 RUNNING, $applied"
echo "[$NAME] 전환 경계: $boundary"
