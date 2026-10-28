# 03. ClickHouse 실시간 적재

## 목적

Kafka로 들어온 주문·체결 이벤트를 ClickHouse에 실시간 적재하고, **중복이 있어도 정확한 분석 결과**를 낼 수 있게 한다.

## 적재 방식 선택

| 방식 | 장점 | 단점 |
|---|---|---|
| **ClickHouse Kafka 엔진 + MV** (채택) | 추가 컴포넌트 없음, ClickHouse 내부에서 완결 | 오프셋·에러 처리를 ClickHouse 설정으로 다뤄야 함 |
| Kafka Connect ClickHouse Sink | Connect 생태계로 일원화, exactly-once 옵션 | 커넥터 추가 운영 |

채택 이유와 대안을 README에 기록한다. 시간이 되면 Sink 방식도 붙여 비교.

## 테이블 구조

```
Kafka 토픽 → [kafka_executions_queue] (Kafka 엔진)
                     │ Materialized View
                     ▼
             [executions] (ReplacingMergeTree)
```

```sql
-- 1) Kafka 소비용 큐 테이블
CREATE TABLE kafka_executions_queue (
  id UInt64, order_id UInt64, account_id UInt64,
  symbol String, side String, price String, quantity UInt32,
  executed_at Int64, __op String, __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'mts.trading.executions',
         kafka_group_name = 'clickhouse-executions',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3;

-- 2) 최종 저장 테이블
CREATE TABLE executions (
  id UInt64, order_id UInt64, account_id UInt64,
  symbol LowCardinality(String), side LowCardinality(String),
  price Decimal(18,4), quantity UInt32,
  executed_at DateTime64(3),
  source_ts DateTime64(3),
  ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(executed_at)
ORDER BY id;

-- 3) 변환 MV
CREATE MATERIALIZED VIEW executions_mv TO executions AS
SELECT id, order_id, account_id, symbol, side,
       toDecimal64(price, 4) AS price, quantity,
       fromUnixTimestamp64Milli(executed_at) AS executed_at,
       fromUnixTimestamp64Milli(__source_ts_ms) AS source_ts
FROM kafka_executions_queue;
```

> **파티션 단위는 월(`toYYYYMM`)**로 둔다 (#49). 처음에는 일 단위였으나, 과거 체결이 섞인 묶음(Debezium 전체 스냅샷 재전송 등)이
> INSERT 한 번에 100개 넘는 일 파티션에 걸쳐 `TOO_MANY_PARTS`로 거부되었고, Kafka 엔진이 커밋 없이 같은 위치를 재시도하며 적재가 멈췄다.
> 파티션은 데이터 관리 단위이고 조회는 `ORDER BY id`로 충분하므로, 데이터 1년 기준 13개 수준이 되는 월 단위가 적절하다.

> 위 컬럼 타입은 02 스펙에서 실제 메시지 포맷을 확인한 뒤 맞춘다. `orders`도 같은 패턴으로 만들되, UPDATE가 반복되므로 `ReplacingMergeTree(updated_at)`로 최신 상태만 남긴다.

## 중복 처리 전략

- Debezium·Kafka 엔진 모두 at-least-once이므로 **중복은 반드시 생긴다**고 가정한다.
- `ReplacingMergeTree`는 백그라운드 머지 때 중복을 제거하므로, 머지 전 조회 시 중복이 보일 수 있다.
- 정확한 집계가 필요할 때 `FINAL` 키워드 사용 vs `argMax`/`uniqExact` 기반 쿼리를 **성능 비교**하고 기록한다.

## 분석 쿼리 예시

- 종목별 1분 단위 거래대금·체결 수
- 최근 10분 거래대금 상위 10개 종목
- 매수/매도 비율 추이

## 지연 측정

`ingested_at - source_ts` = DB 커밋부터 ClickHouse 적재까지의 end-to-end 지연. 분위수(p50, p95, p99)로 집계한다.

```sql
SELECT quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', source_ts, ingested_at))
FROM executions
WHERE ingested_at > now() - INTERVAL 5 MINUTE;
```

## 완료 기준

- [ ] 체결 데이터가 ClickHouse에 실시간 적재됨
- [ ] MySQL과 ClickHouse(`FINAL` 기준)의 체결 건수·수량 합계 일치
- [ ] 500 TPS에서 end-to-end 지연 p95 측정값 기록
- [ ] `FINAL` vs 대안 쿼리 성능 비교 기록
