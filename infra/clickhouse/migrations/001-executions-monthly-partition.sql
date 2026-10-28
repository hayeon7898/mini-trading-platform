-- 001. 체결 테이블 파티션을 일 단위 → 월 단위로 이전 (#49)
--
-- init SQL(01-executions.sql)은 CREATE IF NOT EXISTS라 이미 만들어진 테이블에는 적용되지 않는다.
-- 기존 볼륨을 쓰는 환경에서 한 번만 실행한다. 새로 만드는 환경은 실행할 필요 없음.
--
-- 전제: 체결 큐가 떼어진 상태 (적재 중인 데이터가 섞이지 않게)
--   docker exec mts-clickhouse clickhouse-client -q "DETACH TABLE mts.kafka_executions_queue"
-- 실행:
--   docker exec -i mts-clickhouse clickhouse-client --multiquery < infra/clickhouse/migrations/001-executions-monthly-partition.sql
--
-- 순서 이유
--   - 이름 교체 후 MV를 다시 만든다. MV는 대상 테이블을 내부 ID로 기억할 수 있어, 그대로 두면 예전 테이블에 계속 쓸 위험이 있다.
--   - MV는 큐를 붙인 뒤에 만든다. MV 생성 시 원본(큐) 정의가 필요하고, Kafka 엔진은 연결된 MV가 생기는 순간부터 소비를 시작한다.
--   - 예전 테이블은 executions_daily_bak으로 남긴다. 대조가 일치한 뒤 직접 지운다:
--       DROP TABLE mts.executions_daily_bak

-- 1) 월 단위 파티션의 새 테이블 (컬럼은 01-executions.sql과 동일)
CREATE TABLE mts.executions_new
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
PARTITION BY toYYYYMM(executed_at)
ORDER BY id;

-- 2) 기존 데이터 복사. ingested_at·kafka_ts(E0 지연 측정 값)를 그대로 보존한다.
--    월 단위라 한 번의 INSERT가 걸치는 파티션은 13개 수준 (일 단위로는 같은 복사도 한도 초과)
INSERT INTO mts.executions_new
SELECT id, order_id, account_id, symbol, side, price, quantity, executed_at, source_ts, kafka_ts, ingested_at
FROM mts.executions;

-- 3) 건수 확인. 다르면 에러로 멈춰서 이후 단계(이름 교체)가 실행되지 않는다 (--multiquery는 에러에서 중단)
SELECT throwIf(
    (SELECT count() FROM mts.executions) != (SELECT count() FROM mts.executions_new),
    'executions 복사 건수 불일치: 이름 교체 전에 중단'
) AS copy_check;

-- 4) 이름 교체 + MV 재연결
RENAME TABLE mts.executions TO mts.executions_daily_bak,
             mts.executions_new TO mts.executions;

DROP TABLE IF EXISTS mts.executions_mv;

ATTACH TABLE mts.kafka_executions_queue;

CREATE MATERIALIZED VIEW mts.executions_mv TO mts.executions AS
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

-- 5) 결과 확인: 파티션 키와 파티션 수
SELECT partition_key FROM system.tables WHERE database = 'mts' AND name = 'executions';
SELECT count(DISTINCT partition) AS partitions FROM system.parts WHERE database = 'mts' AND table = 'executions' AND active;
