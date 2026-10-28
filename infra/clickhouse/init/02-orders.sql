-- 주문 최신 상태 적재 (docs/specs/03-clickhouse-sink.md)
--
--   mts.trading.orders (Kafka) → kafka_orders_queue → orders_mv → orders (ReplacingMergeTree(version))
--
-- 주문은 같은 id로 여러 번 갱신된다 (접수 c → 체결마다 u → 취소 u).
-- ReplacingMergeTree는 같은 id 중 version이 가장 큰 행을 남기므로, version이 갱신 순서를 따라야 한다.
--
-- version = updated_at(ms) × 1,000,000 + filled_qty × 8 + 상태 순위
--   - updated_at만 쓰면 같은 밀리초에 접수·체결이 일어날 때 동률이 되어 어느 행이 남을지 보장되지 않는다.
--   - 주문 상태는 한 방향으로만 움직인다: filled_qty는 줄지 않고, 상태는 ACCEPTED → PARTIAL → FILLED/CANCELED.
--     같은 밀리초 안에서는 체결 수량이 많을수록, 상태가 뒤쪽일수록 최신이다.
--   - 범위: updated_at ≈ 1.8e12 × 1e6 = 1.8e18 < UInt64 최대(1.8e19). filled_qty × 8은 filled_qty < 125,000까지 1e6 미만.
--   - 커넥터 설정을 바꾸지 않아 이미 토픽에 있는 과거 이벤트에도 그대로 적용된다.

CREATE DATABASE IF NOT EXISTS mts;

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

CREATE TABLE IF NOT EXISTS mts.orders
(
    id          UInt64,
    account_id  UInt64,
    symbol      LowCardinality(String),
    side        LowCardinality(String),
    price       Decimal(18, 4),
    quantity    UInt32,
    filled_qty  UInt32,
    status      LowCardinality(String),
    created_at  DateTime64(3, 'UTC'),
    updated_at  DateTime64(3, 'UTC'),
    source_ts   DateTime64(3, 'UTC'),
    version     UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
-- 파티션 키는 주문마다 변하지 않는 created_at 기준 (같은 id의 갱신이 같은 파티션에 모여야 중복 제거됨)
PARTITION BY toYYYYMM(created_at)
ORDER BY id;

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
