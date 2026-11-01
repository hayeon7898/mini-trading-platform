-- 002. 주문 큐의 kafka_flush_interval_ms를 체결 큐와 같은 1초로 (#43)
--
-- Kafka 엔진은 ALTER ... MODIFY SETTING을 지원하지 않아 큐를 다시 만든다 (MV 삭제 → 큐 재생성 → MV 재생성).
-- 컨슈머 그룹 이름이 같으므로 Kafka에 커밋된 오프셋부터 이어서 읽고, 적재된 데이터(mts.orders)는 그대로 남는다.
-- 아래 CREATE 두 개는 init/02-orders.sql과 같은 정의다.
-- 실행:
--   docker exec -i mts-clickhouse clickhouse-client --multiquery < infra/clickhouse/migrations/002-orders-queue-flush-interval.sql

DROP TABLE IF EXISTS mts.orders_mv;
DROP TABLE IF EXISTS mts.kafka_orders_queue;

CREATE TABLE IF NOT EXISTS mts.kafka_orders_queue
(
    id             UInt64,
    account_id     UInt64,
    symbol         String,
    side           String,
    price          String,
    quantity       UInt32,
    filled_qty     UInt32,
    status         String,
    created_at     Int64,
    updated_at     Int64,
    __op           String,
    __source_ts_ms Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'mts.trading.orders',
         kafka_group_name = 'clickhouse-orders',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3,
         -- 01-executions.sql과 같은 이유 (백필 중 메모리 한도 초과 방지)
         kafka_max_block_size = 10000,
         kafka_poll_max_batch_size = 10000,
         -- 01-executions.sql과 같은 1초 (#30에서 체결 큐에만 적용되고 빠졌던 것을 #43 E3에서 발견).
         -- 기본 7500ms에서는 소비 재개 후 첫 적재까지 약 16초, 백로그 해소에 약 26초가 걸렸다.
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS mts.orders_mv TO mts.orders AS
SELECT
    id,
    account_id,
    symbol,
    side,
    toDecimal64(price, 4)                          AS price,
    quantity,
    filled_qty,
    status,
    fromUnixTimestamp64Milli(created_at, 'UTC')     AS created_at,
    fromUnixTimestamp64Milli(updated_at, 'UTC')     AS updated_at,
    fromUnixTimestamp64Milli(__source_ts_ms, 'UTC') AS source_ts,
    toUInt64(updated_at) * 1000000
      + toUInt64(filled_qty) * 8
      + multiIf(status = 'ACCEPTED', 0, status = 'PARTIAL', 1, 2) AS version
FROM mts.kafka_orders_queue;

SELECT name, extract(engine_full, 'kafka_flush_interval_ms = [0-9]+') AS flush
FROM system.tables WHERE database = 'mts' AND engine = 'Kafka';
