-- 스펙 01 도메인 모델
-- 인덱스는 스펙 05에서 "개선 전" 상태를 재현하기 위해 PK만 둔다.
USE trading;

CREATE TABLE stocks (
  symbol      VARCHAR(12)   PRIMARY KEY,
  name        VARCHAR(100)  NOT NULL,
  market      VARCHAR(10)   NOT NULL,
  base_price  DECIMAL(18,4) NOT NULL
);

CREATE TABLE accounts (
  id          BIGINT        PRIMARY KEY AUTO_INCREMENT,
  owner_name  VARCHAR(50)   NOT NULL,
  created_at  DATETIME(3)   NOT NULL
);

CREATE TABLE orders (
  id          BIGINT        PRIMARY KEY AUTO_INCREMENT,
  account_id  BIGINT        NOT NULL,
  symbol      VARCHAR(12)   NOT NULL,
  side        VARCHAR(4)    NOT NULL,   -- BUY, SELL
  price       DECIMAL(18,4) NOT NULL,
  quantity    INT           NOT NULL,
  filled_qty  INT           NOT NULL DEFAULT 0,
  status      VARCHAR(16)   NOT NULL,   -- ACCEPTED, PARTIAL, FILLED, CANCELED
  created_at  DATETIME(3)   NOT NULL,
  updated_at  DATETIME(3)   NOT NULL
);

CREATE TABLE executions (
  id          BIGINT        PRIMARY KEY AUTO_INCREMENT,
  order_id    BIGINT        NOT NULL,
  account_id  BIGINT        NOT NULL,
  symbol      VARCHAR(12)   NOT NULL,
  side        VARCHAR(4)    NOT NULL,
  price       DECIMAL(18,4) NOT NULL,
  quantity    INT           NOT NULL,
  executed_at DATETIME(3)   NOT NULL
);