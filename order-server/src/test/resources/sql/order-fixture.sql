-- 주문 API 테스트용 기준 데이터. 매 테스트 전에 비우고 다시 넣는다.
DELETE FROM executions;
DELETE FROM orders;
DELETE FROM accounts;
DELETE FROM stocks;

INSERT INTO stocks (symbol, name, market, base_price)
VALUES ('005930', 'Samsung Electronics', 'KOSPI', 71000.0000);

INSERT INTO accounts (id, owner_name, created_at)
VALUES (1, 'tester', '2026-01-01 00:00:00.000');
