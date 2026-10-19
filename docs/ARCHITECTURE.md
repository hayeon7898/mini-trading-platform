# 미니 증권 주문·체결 플랫폼 — 아키텍처

## 1. 프로젝트 목표

**1차 목표 (~10/25)**: 토스증권 Data Engineer(Realtime) 지원.
증권 주문·체결 원장 DB의 변경분을 CDC로 Kafka에 흘리고, OLAP(ClickHouse)에 실시간 적재하는 파이프라인을 구축한다. 브로커 장애·재시작·부하 실험으로 **정합성과 성능을 수치로 검증**한다.

**2차 목표**: 같은 시스템 위에 백엔드·인프라 에피소드(캐시, 비동기, SSE, GC, 네트워크 분석)를 쌓아 DE/BE/Infra 지원에 재사용한다.

### 원칙

- 툴 개수보다 깊이. 붙인 컴포넌트마다 "왜 이렇게 설계했는가"를 설명할 수 있어야 한다.
- 모든 개선은 **측정 전/후 수치**로 기록한다.
- 실무가 아닌 재현 실험은 문서·이력서에 **"장애 재현 실험"**으로 명시한다.

## 2. 전체 구조

```mermaid
flowchart TD
    K6[k6 부하 생성기] --> OS[Spring Boot 주문 서버]
    OS --> MY[(MySQL<br/>trading DB)]
    MY -- binlog --> DBZ[Debezium<br/>Kafka Connect]
    DBZ --> KF[[Kafka 3-broker<br/>KRaft]]
    KF --> CH[(ClickHouse<br/>Kafka 엔진 → MV)]
    KF -.2단계.-> FL[Flink Job]
    KF -.2단계.-> SSE[WebFlux SSE 서버]
    OS -.2단계.-> RD[(Redis)]
    PR[Prometheus] --> GF[Grafana]
    KF --> PR
    OS --> PR
```

## 3. 컴포넌트

| 컴포넌트 | 역할 | 기술 | 단계 |
|---|---|---|---|
| order-server | 주문 접수, 체결 시뮬레이션, 조회 API | Java 21, Spring Boot 3, JPA | 1 |
| MySQL | 주문·체결 원장 DB (CDC 원천) | MySQL 8 | 1 |
| Debezium | binlog 기반 CDC → Kafka | Kafka Connect + Debezium MySQL | 1 |
| Kafka | 이벤트 버스, 3 브로커 클러스터 | Apache Kafka (KRaft) | 1 |
| ClickHouse | 실시간 분석 저장소 | Kafka 엔진 + Materialized View | 1 |
| 모니터링 | 처리량·지연·lag 관측 | Prometheus, Grafana, kafka-exporter | 1 |
| 부하 생성 | 주문 트래픽 생성, API 성능 측정 | k6 | 1 |
| Flink Job | 종목별 실시간 집계 | Flink DataStream (Java) | 2 |
| SSE 서버 | 체결 알림 실시간 푸시 | Spring WebFlux (Netty) | 2 |
| Redis | 잔고 캐시 | Redis | 2 |

## 4. 데이터 흐름 — 주문 1건의 일생

1. 클라이언트(k6)가 `POST /orders`로 주문을 보낸다.
2. order-server가 `orders`에 `status=ACCEPTED`로 저장한다.
3. 체결 시뮬레이터가 접수된 주문을 읽어 `executions`를 생성하고 `orders.status`를 갱신한다.
4. MySQL binlog를 Debezium이 읽어 `mts.trading.orders`, `mts.trading.executions` 토픽에 발행한다.
5. ClickHouse Kafka 엔진 테이블이 토픽을 소비하고, Materialized View가 `ReplacingMergeTree` 테이블에 적재한다.
6. 분석 쿼리(종목별 거래대금 등)를 ClickHouse에서 조회한다.

> 서버가 DB와 Kafka에 각각 쓰지 않고(dual write) CDC를 쓰는 이유: DB 커밋과 이벤트 발행의 원자성을 보장하기 위해. 면접 단골 질문이니 README에 정리할 것.

## 5. Kafka 설계 (초안)

| 항목 | 값 | 비고 |
|---|---|---|
| 브로커 수 | 3 (KRaft combined mode) | 로컬 리소스 고려 |
| replication.factor | 3 | |
| min.insync.replicas | 2 | 실험 E1에서 1과 비교 |
| 파티션 수 | 6 | 실험 E4에서 3/6/12 비교 |
| 메시지 키 | 테이블 PK (Debezium 기본) | 계좌 단위 순서 보장은 2단계 과제 |

| 토픽 | 생산자 | 소비자 |
|---|---|---|
| `mts.trading.orders` | Debezium | ClickHouse |
| `mts.trading.executions` | Debezium | ClickHouse, (2단계) Flink, SSE |
| `connect-*`, `schema-history.mts` | Kafka Connect 내부 | — |

## 6. 기술 스택 및 버전

작성 시점의 안정 버전으로 **정확한 버전을 고정**한다 (`latest` 태그 금지). 정한 버전은 이 표에 기록한다.

| 구성 | 버전 |역할|
|---|---|---|
| Java / Spring Boot | `21 / 4.1.1`|-|
| MySQL | `8.x` |-|
| Kafka | (기록) |-|
| Debezium | (기록) |-|
| ClickHouse | (기록) |-|
| k6 | `grafana/k6:2.3.0` | 부하 생성 (constant-arrival-rate) |
| Prometheus | `prom/prometheus:v3.15.0` | order-server 지표 수집 |

## 7. 디렉토리 구조

```
mini-trading-platform/
├── docker-compose.yml
├── infra/
│   ├── mysql/          # my.cnf, init.sql (DDL, Debezium 계정)
│   ├── connect/        # 커넥터 설정 JSON, 등록 스크립트
│   ├── clickhouse/     # init SQL (Kafka 엔진, MV, 타깃 테이블)
│   ├── prometheus/
│   └── grafana/
├── order-server/       # Spring Boot
├── load-test/          # k6 스크립트
├── experiments/
│   ├── producer-tool/  # acks 실험용 Java 프로듀서
│   ├── scripts/        # 장애 주입 스크립트
│   └── results/        # 실험 결과 기록
└── docs/
    ├── ARCHITECTURE.md
    └── specs/
```

## 8. 로컬 리소스

Kafka 3대 + Connect + ClickHouse는 메모리를 많이 쓴다. Docker에 **최소 8GB** 할당을 권장하고, 각 컨테이너 JVM 힙을 명시적으로 제한한다 (예: Kafka `-Xmx512m`, Connect `-Xmx1g`).

## 9. 단계별 계획

### 1단계 (10/14 ~ 10/25)

| 날짜 | 작업 | 스펙 |
|---|---|---|
| 10/14~15 | docker-compose 구성, order-server 뼈대 | — |
| 10/16~17 | 주문·체결 API, 부하 생성기, Debezium CDC | [01](specs/01-order-server.md), [02](specs/02-cdc-pipeline.md) |
| 10/18~19 | ClickHouse 적재, 분석 쿼리 | [03](specs/03-clickhouse-sink.md) |
| 10/20 | 주문 내역 조회 쿼리 개선 | [05](specs/05-query-optimization.md) |
| 10/21~22 | 장애·부하 실험 | [04](specs/04-reliability-experiments.md) |
| 10/23 | README 작성 | — |
| 10/24 | 이력서 작성 | — |
| 10/25 | 버퍼, 오후 중 제출 | — |

### 2단계 이후

[06-phase2-backlog.md](specs/06-phase2-backlog.md) 참고.

## 10. 실험 기록 원칙

모든 실험은 `experiments/results/YYYYMMDD-<실험ID>.md`에 아래 형식으로 남긴다.

- **가설**: 무엇이 일어날 거라 예상했는가
- **조건**: 설정값, 데이터량, TPS
- **결과**: 수치, 그래프 캡처
- **해석**: 왜 그런 결과가 나왔는가, 예상과 달랐다면 왜인가
- **조치**: 무엇을 바꿨고 그 후 수치는 어떻게 변했는가
