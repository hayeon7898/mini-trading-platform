-- 체결 실시간 적재 (docs/specs/03-clickhouse-sink.md)
--
--   mts.trading.executions (Kafka) → kafka_executions_queue (Kafka 엔진)
--                                  → executions_mv (Materialized View)
--                                  → executions (ReplacingMergeTree)
--
-- 메시지 형식 (#11, #12에서 확인): price는 문자열 "71000.0000", executed_at·__source_ts_ms는 epoch millis(UTC)
-- 시각 컬럼은 DateTime64(3, 'UTC')로 시간대를 명시한다 (#11의 9시간 시각 버그 교훈).
-- Kafka 엔진은 MV 삽입이 끝난 뒤 오프셋을 커밋하므로 at-least-once. 중복은 ReplacingMergeTree(ORDER BY id)가 머지 때 제거하고,
-- 머지 전 정확한 집계가 필요하면 FINAL을 쓴다.
--
-- 지연 구간 (#30)
--   source_ts   : MySQL 커밋 시각 (Debezium source.ts_ms)
--   kafka_ts    : Kafka에 기록된 시각 (Kafka 엔진 가상 컬럼 _timestamp_ms)
--   ingested_at : ClickHouse에 들어간 시각
--   kafka_ts - source_ts   = Debezium 읽기·발행
--   ingested_at - kafka_ts = ClickHouse 소비·묶음 대기 (kafka_flush_interval_ms의 영향)

CREATE DATABASE IF NOT EXISTS mts;

-- 1) Kafka 소비용 큐. 직접 SELECT하면 메시지를 소비해 버리므로 조회하지 않는다.
CREATE TABLE IF NOT EXISTS mts.kafka_executions_queue
(
    id             UInt64,
    order_id       UInt64,
    account_id     UInt64,
    symbol         String,
    side           String,
    price          String,
    quantity       UInt32,
    executed_at    Int64,
    __op           String,
    __source_ts_ms Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'mts.trading.executions',
         kafka_group_name = 'clickhouse-executions',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3,
         -- 백필 중 컨슈머 3개가 큰 묶음을 동시에 들고 메모리 한도(약 1.8GiB)를 넘어 같은 위치를 재시도하며 멈춤.
         -- 묶음을 줄여 메모리 사용을 낮춘다.
         kafka_max_block_size = 10000,
         kafka_poll_max_batch_size = 10000,
         -- 모은 메시지를 넣는 최대 간격. 기본 7500 → 1000 (#30: p95 7.6초 → 1.7초, 500보다 삽입 횟수 절반)
         kafka_flush_interval_ms = 1000;

-- 2) 최종 저장 테이블. 같은 id가 여러 번 들어오면 머지 때 하나만 남는다.
CREATE TABLE IF NOT EXISTS mts.executions
(
    id          UInt64,
    order_id    UInt64,
    account_id  UInt64,
    symbol      LowCardinality(String),
    side        LowCardinality(String),
    price       Decimal(18, 4),
    quantity    UInt32,
    executed_at DateTime64(3, 'UTC'),
    source_ts   DateTime64(3, 'UTC'),
    kafka_ts    DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
-- 월 단위 파티션 (#49). 일 단위였을 때 과거 체결이 섞인 묶음(스냅샷 재전송 등)이 INSERT 한 번에
-- 100개 넘는 파티션에 걸쳐 TOO_MANY_PARTS로 거부되고, Kafka 엔진이 같은 위치를 무한히 재시도하며 적재가 멈췄다.
-- 파티션은 데이터 관리(DROP PARTITION 등) 단위이고 조회는 ORDER BY id로 충분하므로 월 단위로 둔다 (주문과 동일).
-- 같은 id는 항상 같은 executed_at → 같은 파티션이라 ReplacingMergeTree의 중복 제거에 영향 없음.
PARTITION BY toYYYYMM(executed_at)
ORDER BY id;

-- 이미 만들어진 테이블에 kafka_ts 추가 (#30). 새로 만드는 경우에는 아무 일도 하지 않는다.
ALTER TABLE mts.executions
    ADD COLUMN IF NOT EXISTS kafka_ts DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC') AFTER source_ts;

-- 3) 변환 MV. 큐에 들어온 메시지를 타입 변환해 executions에 넣는다.
CREATE MATERIALIZED VIEW IF NOT EXISTS mts.executions_mv TO mts.executions AS
SELECT
    id,
    order_id,
    account_id,
    symbol,
    side,
    toDecimal64(price, 4)                                        AS price,
    quantity,
    fromUnixTimestamp64Milli(executed_at, 'UTC')                  AS executed_at,
    fromUnixTimestamp64Milli(__source_ts_ms, 'UTC')               AS source_ts,
    toDateTime64(ifNull(_timestamp_ms, toDateTime64(0, 3)), 3, 'UTC') AS kafka_ts
FROM mts.kafka_executions_queue;