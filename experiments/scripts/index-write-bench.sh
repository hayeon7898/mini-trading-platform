#!/usr/bin/env bash
# 인덱스 추가가 주문 접수(쓰기)에 주는 영향 (#23). 저장소 루트에서 실행.
#   bash experiments/scripts/index-write-bench.sh
#
# 1) 인덱스 삭제 → 밀린 체결이 끝날 때까지 대기 → 500 TPS 2분
# 2) 열린 주문 0인 상태에서 인덱스 추가, DDL 전후 orders 토픽 메시지 수 기록
# 3) 밀린 체결 대기 → 500 TPS 2분
# 전제: compose의 order-server(mts-order-server)가 실행 중
set -euo pipefail

INDEX=idx_orders_account_created
DDL="CREATE INDEX $INDEX ON orders (account_id, created_at)"
RATE="${RATE:-500}"
DURATION="${DURATION:-2m}"
DATE=$(date +%Y%m%d)
OUT="${OUT:-experiments/results/${DATE}-W1-index-write-impact}/raw"
NET=$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
mkdir -p "$OUT"

sql() {
  docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'
}

topic_offset() {
  docker exec mts-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka-1:9092 --topic mts.trading.orders | awk -F: '{s+=$3} END {print s}'
}

index_count() {
  sql <<< "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS
           WHERE TABLE_SCHEMA = 'trading' AND TABLE_NAME = 'orders' AND INDEX_NAME = '$INDEX';"
}

wait_idle() {
  local open
  while true; do
    open=$(sql <<< "SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL');")
    [[ "$open" == "0" ]] && return
    echo "  밀린 체결 대기: 열린 주문 ${open}건"
    sleep 5
  done
}

load() { # $1=라벨
  local file="$OUT/${DATE}-W1-k6-$1.txt"
  docker run --rm -i --network "$NET" -v "$PWD/load-test:/scripts" grafana/k6:2.3.0 run --quiet \
    -e BASE_URL=http://mts-order-server:8080 -e RATE="$RATE" -e DURATION="$DURATION" \
    /scripts/orders.js 2>&1 | sed -n '/THRESHOLDS/,$p' > "$file"
  grep -E "http_reqs|http_req_duration\.|dropped_iterations" "$file"
}

echo "== 0) 인덱스 삭제"
if [[ "$(index_count)" != "0" ]]; then
  sql <<< "DROP INDEX $INDEX ON orders;"
fi
echo "  $INDEX 개수: $(index_count) (0이어야 함)"
wait_idle

echo "== 1) 인덱스 없이 ${RATE} TPS ${DURATION}"
load no-index
wait_idle

echo "== 2) 인덱스 추가 (열린 주문 0인 상태)"
before=$(topic_offset)
start=$SECONDS
sql <<< "$DDL; ANALYZE TABLE orders;" > /dev/null
elapsed=$(( SECONDS - start ))
sleep 10
after=$(topic_offset)
{
  echo "인덱스 생성 시간: ${elapsed}s"
  echo "DDL 전후 orders 토픽 메시지 수: ${before} -> ${after}"
} | tee "$OUT/${DATE}-W1-cdc.txt"
echo "  $INDEX 개수: $(index_count) (1이어야 함)"
wait_idle

echo "== 3) 인덱스 추가 후 ${RATE} TPS ${DURATION}"
load with-index

echo "원본: $OUT"
