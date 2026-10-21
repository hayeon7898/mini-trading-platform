#!/usr/bin/env bash
# 05 스펙(조회 쿼리 개선) 실험용 과거 데이터 적재.
#   bash infra/mysql/seed/seed.sh               # 주문 100만 건
#   ORDERS=300000 bash infra/mysql/seed/seed.sh # 규모 조정 (10만 단위)
#
# - 종목 100개, 계좌 10,000개, 과거 1년치 주문, FILLED 주문의 체결 기록
# - 모든 세션에서 sql_log_bin=0: binlog에 남기지 않아 Debezium(CDC)이 읽지 않는다.
#   05는 MySQL 쿼리 실험이라 Kafka·ClickHouse에 과거 데이터가 필요 없다.
# - 과거 주문은 모두 닫힌 상태(FILLED, CANCELED)라 체결 시뮬레이터가 건드리지 않는다.
# - FILLED 주문마다 같은 수량의 체결 1건을 만들어 SUM(executions.quantity) = filled_qty를 유지한다.
set -euo pipefail

ORDERS="${ORDERS:-1000000}"
CHUNK=100000
CHUNKS=$(( ORDERS / CHUNK ))

sql() {
  # 매 호출이 새 세션이라 sql_log_bin=0을 매번 건다.
  # MYSQL_PWD로 넘겨 비밀번호 경고 없이, 오류 메시지는 그대로 보이게 한다.
  { echo "SET SESSION sql_log_bin = 0;"; cat; } \
    | docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'
}

SECONDS=0
START_ID=$(echo "SELECT COALESCE(MAX(id), 0) FROM orders;" | sql)
echo "[seed] 주문 ${ORDERS}건 적재 시작 (기존 최대 주문 id: ${START_ID})"

# ---------------------------------------------------------------- 숫자 테이블, 종목, 계좌
sql <<'SQL'
DROP TABLE IF EXISTS seed_nums;
CREATE TABLE seed_nums (n INT PRIMARY KEY);
INSERT INTO seed_nums
SELECT a.d + b.d * 10 + c.d * 100 + d.d * 1000 + e.d * 10000
FROM (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
CROSS JOIN (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) e;

-- 종목 S0001~S0100. 기준가는 번호 × 1,000원 (1,000 ~ 100,000)
INSERT IGNORE INTO stocks (symbol, name, market, base_price)
SELECT CONCAT('S', LPAD(n, 4, '0')), CONCAT('Stock ', n),
       ELT(1 + n % 3, 'KOSPI', 'KOSDAQ', 'NASDAQ'), 1000 * n
FROM seed_nums WHERE n BETWEEN 1 AND 100;

-- 계좌 1~10,000
INSERT IGNORE INTO accounts (id, owner_name, created_at)
SELECT n, CONCAT('user', n), UTC_TIMESTAMP(3) - INTERVAL 400 DAY
FROM seed_nums WHERE n BETWEEN 1 AND 10000;
SQL
echo "[seed] 종목·계좌 완료 (${SECONDS}s)"

# ---------------------------------------------------------------- 주문 (10만 건씩)
# account_id = 1 + FLOOR(RAND()^3 × 10000): 번호가 작은 계좌에 몰리게 해 "주문이 많은 계좌"를 만든다.
#   계좌 1이 약 4.6%를 가져간다 (100만 건 기준 약 4.6만 건).
# 상태: FILLED 75% (filled_qty = quantity), CANCELED 25% (filled_qty = 0)
# 시각: 최근 1년 안에서 균등 분포 (UTC)
# 파생 테이블의 LIMIT은 병합을 막아 RAND()가 행마다 한 번만 계산되게 한다
# (병합되면 si를 쓰는 symbol과 price에서 RAND()가 따로 계산될 수 있음).
for i in $(seq 1 "$CHUNKS"); do
  sql <<'SQL'
INSERT INTO orders (account_id, symbol, side, price, quantity, filled_qty, status, created_at, updated_at)
SELECT acc,
       CONCAT('S', LPAD(si, 4, '0')),
       side,
       ROUND(1000 * si * (1 + (pr - 0.5) * 0.02), 4),
       qty,
       IF(st = 'FILLED', qty, 0),
       st,
       ca,
       ca + INTERVAL up SECOND
FROM (
  SELECT 1 + FLOOR(POW(RAND(), 3) * 10000)              AS acc,
         1 + FLOOR(RAND() * 100)                         AS si,
         IF(RAND() < 0.5, 'BUY', 'SELL')                 AS side,
         RAND()                                          AS pr,
         1 + FLOOR(RAND() * 100)                         AS qty,
         IF(RAND() < 0.75, 'FILLED', 'CANCELED')         AS st,
         UTC_TIMESTAMP(3) - INTERVAL FLOOR(RAND() * 365 * 86400) SECOND AS ca,
         1 + FLOOR(RAND() * 60)                          AS up
  FROM seed_nums
  LIMIT 100000
) t;
SQL
  echo "[seed] 주문 $(( i * CHUNK ))/${ORDERS} (${SECONDS}s)"
done

# ---------------------------------------------------------------- 체결, 통계, 정리
# 주문 삽입 직후의 최대 id를 시드 범위의 끝으로 잡는다.
# 대량 INSERT ... SELECT는 AUTO_INCREMENT를 묶음으로 미리 잡고 남은 id를 버려, 범위가 START_ID + ORDERS보다 뒤까지 퍼진다.
END_ID=$(echo "SELECT MAX(id) FROM orders;" | sql)

sql <<SQL
INSERT INTO executions (order_id, account_id, symbol, side, price, quantity, executed_at)
SELECT id, account_id, symbol, side, price, quantity, updated_at
FROM orders WHERE id BETWEEN $(( START_ID + 1 )) AND ${END_ID} AND status = 'FILLED';
ANALYZE TABLE orders, executions;
DROP TABLE seed_nums;
SQL
echo "[seed] 체결 완료 (${SECONDS}s)"
echo "[seed] 시드 주문 id 범위: $(( START_ID + 1 )) ~ ${END_ID} (reconcile-executions.sh의 SEED_ORDER_MIN/MAX)"

echo "[seed] 결과"
sql <<'SQL'
SELECT 'stocks', COUNT(*) FROM stocks
UNION ALL SELECT 'accounts', COUNT(*) FROM accounts
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'executions', COUNT(*) FROM executions
UNION ALL SELECT 'account 1 orders', COUNT(*) FROM orders WHERE account_id = 1;
SQL
echo "[seed] 총 소요 ${SECONDS}s"

