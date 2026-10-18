#!/usr/bin/env bash
# infra/connect/*.json 을 Kafka Connect에 등록한다. 파일 이름이 커넥터 이름이다.
# PUT /connectors/{name}/config 는 없으면 만들고 있으면 갱신하므로 여러 번 실행해도 안전하다.
# 전제: 토픽이 먼저 만들어져 있어야 한다 (scripts/create-topics.sh).
#   bash infra/connect/register.sh
#   CONNECT_URL=http://localhost:8083 bash infra/connect/register.sh
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
dir="$(cd "$(dirname "$0")" && pwd)"

for f in "$dir"/*.json; do
  name="$(basename "$f" .json)"
  code=$(curl -s -o /tmp/connect-resp.json -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' --data @"$f" \
    "$CONNECT_URL/connectors/$name/config")
  if [[ "$code" -ge 400 ]]; then
    echo "[$name] 등록 실패 (HTTP $code)"
    cat /tmp/connect-resp.json; echo
    exit 1
  fi
  echo "[$name] 등록 완료 (HTTP $code)"
done

sleep 3
for f in "$dir"/*.json; do
  name="$(basename "$f" .json)"
  echo "[$name] 상태:"
  curl -s "$CONNECT_URL/connectors/$name/status"; echo
done
