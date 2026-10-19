# Q0. 계좌별 주문 내역 조회 — 개선 전 기준선

05 스펙(주문 내역 조회 쿼리 개선)의 "개선 전" 측정. 인덱스는 PK와 `idx_orders_status_id (status, id)`뿐이다.

## 가설
`account_id`, `created_at`을 포함한 인덱스가 없으므로 계좌별 조회는 테이블 전체를 읽는다.
따라서 응답 시간은 계좌의 주문 수와 무관하게 테이블 크기에 비례할 것이다.

## 조건
| 항목 | 값 |
|---|---|
| 환경 | 로컬 WSL2 Docker, MySQL 8.4.8 (컨테이너 메모리 상한 1GB) |
| 데이터 | `orders` 1,000,029건 (최근 1년), `executions` 750,135건 |
| 데이터 분포 | 계좌 번호가 작을수록 주문 집중. 계좌 1: 46,500건, 계좌 5000: 소수 |
| 쿼리 | `WHERE account_id = ? AND created_at >= now-30d AND created_at < now ORDER BY created_at DESC, id DESC LIMIT 20` (API가 실행하는 것과 동일) |
| API 측정 | `GET /accounts/{id}/orders?size=20`, curl `time_total`, 35회 중 앞 5회 워밍업 제외 30회. 시뮬레이터 꺼짐 |

## 결과

### 실행 계획 (`EXPLAIN ANALYZE`)
| 계좌 | 접근 방식 | 읽은 행 | 조건 통과 | 정렬 | 쿼리 시간 |
|---|---|---|---|---|---|
| 1 | Table scan | 1,000,000 | 3,860 | filesort (top-20) | 481ms |
| 5000 | Table scan | 1,000,000 | 3 | filesort (top-20) | 487ms |

### API 응답 시간 (30회)
| 계좌 | p50 | p95 | max |
|---|---|---|---|
| 1 | 443ms | 522ms | 556ms |
| 5000 | 439ms | 472ms | 490ms |

## 해석
- 두 계좌 모두 100만 행 전체를 읽고(테이블 스캔 약 420ms), 조건에 맞는 행을 정렬해 20건을 고른다.
- 결과가 3건인 계좌 5000도 3,860건인 계좌 1과 시간이 같다. 비용이 결과 수가 아니라 테이블 크기에 비례한다.
- 테이블이 커지면(예: 1,000만 건) 응답 시간도 비례해 늘어날 것으로 예상된다.

## 조치 (05에서 진행)
- `(account_id, created_at)` 인덱스 추가.
  InnoDB 보조 인덱스에는 PK(`id`)가 뒤에 붙으므로 실제 순서는 `(account_id, created_at, id)`이고,
  `ORDER BY created_at DESC, id DESC`를 인덱스 역방향 스캔으로 처리해 filesort 없이 20건에서 멈출 것으로 예상한다.
- 상태 필터 쿼리용 `(account_id, status, created_at)` 추가 여부는 인덱스 개수·쓰기 비용과 함께 비교한다.
