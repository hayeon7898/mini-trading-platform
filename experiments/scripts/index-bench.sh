#!/usr/bin/env bash
# 계좌별 주문 조회 인덱스 후보 비교 (#22). 저장소 루트에서 실행.
#   bash experiments/scripts/index-bench.sh
#
# 후보마다: 인덱스 추가 → ANALYZE → 실행 계획 → k6(워밍업 10초 + 본측정) → 인덱스 삭제
# 결과는 experiments/results/raw/ 에 저장하고, 마지막에 요약 표를 출력한다.
# 전제: compose의 order-server(mts-order-server)가 실행 중
set -euo pipefail

DATE=$(date +%Y%m%d)
RAW=experiments/results/raw
K6=grafana/k6:2.3.0
DURATION="${DURATION:-1m}"
NET=$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
mkdir -p "$RAW"

sql() {
  docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading'
}

# 이름|DDL. 이름이 none이면 인덱스 없이 측정
CANDIDATES=(
  "none|"
  "a|CREATE INDEX idx_bench ON orders (account_id)"
  "ac|CREATE INDEX idx_bench ON orders (account_id, created_at)"
  "asc|CREATE INDEX idx_bench ON orders (account_id, status, created_at)"
  "cover|CREATE INDEX idx_bench ON orders (account_id, created_at, status, symbol, side, price, quantity, filled_qty, updated_at)"
)

# API가 실행하는 쿼리와 같은 모양
WHERE30="created_at >= UTC_TIMESTAMP(3) - INTERVAL 30 DAY AND created_at < UTC_TIMESTAMP(3)"
Q_HEAVY="SELECT * FROM orders WHERE account_id = 1 AND $WHERE30 ORDER BY created_at DESC, id DESC LIMIT 20"
Q_STATUS="SELECT * FROM orders WHERE account_id = 1 AND status = 'CANCELED' AND $WHERE30 ORDER BY created_at DESC, id DESC LIMIT 20"
Q_LIGHT="SELECT * FROM orders WHERE account_id = 5000 AND $WHERE30 ORDER BY created_at DESC, id DESC LIMIT 20"

k6run() { # $1=라벨 $2=ACCOUNT $3=STATUS
  local out="$RAW/${DATE}-Q1-k6-$1.txt"
  docker run --rm -i --network "$NET" -v "$PWD/load-test:/scripts" "$K6" run --quiet \
    -e ACCOUNT="$2" -e STATUS="$3" -e DURATION=10s /scripts/account-orders.js >/dev/null 2>&1 || true
  docker run --rm -i --network "$NET" -v "$PWD/load-test:/scripts" "$K6" run --quiet \
    -e ACCOUNT="$2" -e STATUS="$3" -e DURATION="$DURATION" /scripts/account-orders.js > "$out" 2>&1 || true
  local reqs dur
  reqs=$(grep -m1 'http_reqs' "$out" | awk '{print $3}')
  dur=$(grep -m1 'http_req_duration' "$out" | grep -oE 'p\(50\)=[^ ]+|p\(95\)=[^ ]+' | tr '\n' ' ')
  echo "$reqs $dur"
}

SUMMARY="$RAW/${DATE}-Q1-summary.txt"
: > "$SUMMARY"

sql <<< "DROP INDEX idx_bench ON orders;" 2>/dev/null || true

for c in "${CANDIDATES[@]}"; do
  name="${c%%|*}"; ddl="${c#*|}"
  echo "===== $name ====="

  if [[ -n "$ddl" ]]; then
    start=$SECONDS
    sql <<< "$ddl; ANALYZE TABLE orders;" >/dev/null
    echo "  인덱스 생성 $(( SECONDS - start ))s"
  fi

  {
    echo "### $name: $ddl"
    echo "-- 인덱스 크기 (bytes)"
    echo "SELECT index_name, stat_value * @@innodb_page_size FROM mysql.innodb_index_stats
          WHERE database_name='trading' AND table_name='orders' AND stat_name='size';" | sql
    for q in "$Q_HEAVY" "$Q_STATUS" "$Q_LIGHT"; do
      echo; echo "-- $q"
      echo "EXPLAIN $q;" | sql
      echo "EXPLAIN ANALYZE $q\G" | sql
    done
  } > "$RAW/${DATE}-Q1-explain-$name.txt"

  heavy=$(k6run "$name-heavy" 1 "")
  light=$(k6run "$name-random" random "")
  status=$(k6run "$name-status" 1 CANCELED)
  printf '%-6s | heavy  %s\n%-6s | random %s\n%-6s | status %s\n' \
    "$name" "$heavy" "$name" "$light" "$name" "$status" | tee -a "$SUMMARY"

  if [[ -n "$ddl" ]]; then
    sql <<< "DROP INDEX idx_bench ON orders;"
  fi
done

echo
echo "요약: $SUMMARY (열: 초당 요청 수, p50, p95)"
cat "$SUMMARY"
