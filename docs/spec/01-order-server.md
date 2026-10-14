# 01. 주문 서버 (order-server)

## 목적

CDC 파이프라인의 원천이 되는 **증권 원장 DB에 현실적인 쓰기 트래픽을 만든다.** 실제 거래소 매칭 엔진을 만드는 게 목적이 아니므로 체결 로직은 단순하게 유지한다.

## 범위

- 포함: 주문 접수, 체결 시뮬레이션, 주문·체결 조회 API, 시드 데이터
- 제외: 인증/인가, 실제 호가 매칭, 잔고 차감 정합성(2단계 Redis 에피소드에서 다룸)

## 도메인 모델 (MySQL DDL 초안)

```sql
CREATE TABLE stocks (
  symbol      VARCHAR(12) PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  market      VARCHAR(10)  NOT NULL,      -- KOSPI, KOSDAQ, NASDAQ ...
  base_price  DECIMAL(18,4) NOT NULL
);

CREATE TABLE accounts (
  id          BIGINT PRIMARY KEY AUTO_INCREMENT,
  owner_name  VARCHAR(50) NOT NULL,
  created_at  DATETIME(3) NOT NULL
);

CREATE TABLE orders (
  id          BIGINT PRIMARY KEY AUTO_INCREMENT,
  account_id  BIGINT NOT NULL,
  symbol      VARCHAR(12) NOT NULL,
  side        VARCHAR(4)  NOT NULL,       -- BUY, SELL
  price       DECIMAL(18,4) NOT NULL,
  quantity    INT NOT NULL,
  filled_qty  INT NOT NULL DEFAULT 0,
  status      VARCHAR(16) NOT NULL,       -- ACCEPTED, PARTIAL, FILLED, CANCELED
  created_at  DATETIME(3) NOT NULL,
  updated_at  DATETIME(3) NOT NULL
  -- 인덱스는 05 스펙에서 "개선 전" 상태를 재현하기 위해 일부러 최소화
);

CREATE TABLE executions (
  id          BIGINT PRIMARY KEY AUTO_INCREMENT,
  order_id    BIGINT NOT NULL,
  account_id  BIGINT NOT NULL,
  symbol      VARCHAR(12) NOT NULL,
  side        VARCHAR(4)  NOT NULL,
  price       DECIMAL(18,4) NOT NULL,
  quantity    INT NOT NULL,
  executed_at DATETIME(3) NOT NULL
);
```

## API

| Method | Path | 설명 |
|---|---|---|
| POST | `/orders` | 주문 접수 → `ACCEPTED` 저장 후 주문 ID 반환 |
| POST | `/orders/{id}/cancel` | 미체결 잔량 취소 |
| GET | `/orders/{id}` | 주문 단건 조회 |
| GET | `/accounts/{id}/orders?from=&to=&status=&size=` | 계좌별 주문 내역 (05 스펙 개선 대상) |
| GET | `/accounts/{id}/executions?from=&to=` | 계좌별 체결 내역 |

## 체결 시뮬레이터

- 주기 실행(예: 100ms)으로 `ACCEPTED`/`PARTIAL` 주문을 일정 개수씩 가져와 체결을 생성한다.
- 체결 수량은 잔량 이하에서 랜덤, 체결가는 `base_price ± 랜덤 변동`.
- 체결 생성과 주문 상태 갱신은 **하나의 트랜잭션**으로 처리한다.
- 동시 처리 시 같은 주문을 두 번 체결하지 않도록 `SELECT ... FOR UPDATE SKIP LOCKED` 사용을 검토한다. (선택 이유를 README에 기록)

## 시드 데이터

- 종목 100개, 계좌 10,000개
- 05 스펙 쿼리 실험용으로 과거 주문 약 1,000만 건 (별도 스크립트, 배치 INSERT)

## 부하 생성 (k6)

- `constant-arrival-rate` 시나리오로 초당 N건 주문 (100 → 500 → 1,000 TPS 단계 상승)
- 계좌·종목은 랜덤 선택, 일부 종목에 트래픽이 몰리도록 편향을 준다 (핫 파티션 실험용)

## 완료 기준

- [ ] k6로 500 TPS 주문을 5분간 넣었을 때 에러율 1% 미만
- [ ] 체결 시뮬레이터가 주문을 중복 체결하지 않음 (`SUM(executions.quantity) = orders.filled_qty` 검증 쿼리 통과)
- [ ] Prometheus에서 API 지연시간(p95) 확인 가능 (Spring Actuator + Micrometer)

## 기록할 것

- 체결 시뮬레이터의 동시성 처리 방식과 그 이유
- 서버 처리량 한계가 어디서 왔는지 (DB 커넥션 풀, 락 경합 등)
