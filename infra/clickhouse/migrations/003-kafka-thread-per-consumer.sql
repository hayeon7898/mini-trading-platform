-- 003. 두 Kafka 큐에 kafka_thread_per_consumer = 1 적용 (#43 E3)
--
-- Kafka 엔진은 ALTER ... MODIFY SETTING을 지원하지 않아 큐를 다시 만든다 (MV 삭제 → 큐 재생성 → MV 재생성).
-- 컨슈머 그룹 이름이 같으므로 커밋된 오프셋부터 이어서 읽고, 적재된 데이터는 대상 테이블에 그대로 남는다.
-- 아래 CREATE는 init/01-executions.sql, init/02-orders.sql과 같은 정의다 (002의 주문 큐 정의를 대체).
-- 재생성 직후 예전 컨슈머의 세션 만료(약 45초)까지 소비가 멈출 수 있다.
-- 실행:
--   docker exec -i mts-clickhouse clickhouse-client --multiquery < infra/clickhouse/migrations/003-kafka-thread-per-consumer.sql

DROP TABLE IF EXISTS mts.executions_mv;
DROP TABLE IF EXISTS mts.kafka_executions_queue;
DROP TABLE IF EXISTS mts.orders_mv;
DROP TABLE IF EXISTS mts.kafka_orders_queue;

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
         -- 컨슈머마다 스레드 (#43 E3). 기본 0은 스레드 하나가 컨슈머 셋을 돌아가며 처리해, 소비 재개·재생성 시
         -- 컨슈머가 차례로 합류할 때 두 번째 재조정이 약 16초 지연됐다 (중단 5분 80 TPS: 해소 중앙값 18.2초 → 2.3초).
         kafka_thread_per_consumer = 1,
         -- 백필 중 컨슈머 3개가 큰 묶음을 동시에 들고 메모리 한도(약 1.8GiB)를 넘어 같은 위치를 재시도하며 멈춤.
         -- 묶음을 줄여 메모리 사용을 낮춘다.
         kafka_max_block_size = 10000,
         kafka_poll_max_batch_size = 10000,
         -- 모은 메시지를 넣는 최대 간격. 기본 7500 → 1000 (#30: p95 7.6초 → 1.7초, 500보다 삽입 횟수 절반)
         kafka_flush_interval_ms = 1000;

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
         -- 컨슈머마다 스레드 (#43 E3). 기본 0은 스레드 하나가 컨슈머 셋을 돌아가며 처리해, 소비 재개·재생성 시
         -- 컨슈머가 차례로 합류할 때 두 번째 재조정이 약 16초 지연됐다 (중단 5분 80 TPS: 해소 중앙값 18.2초 → 2.3초).
         kafka_thread_per_consumer = 1,
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

SELECT name,
       extract(engine_full, 'kafka_thread_per_consumer = [0-9]+') AS tpc,
       extract(engine_full, 'kafka_flush_interval_ms = [0-9]+') AS flush
FROM system.tables WHERE database = 'mts' AND engine = 'Kafka';
