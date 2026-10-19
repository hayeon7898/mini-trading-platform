# Q1. 계좌별 주문 내역 조회 — 인덱스 후보 비교

Q0(개선 전 기준선)에 이어 인덱스 후보 4개를 비교하고 최종 인덱스를 고른다 (05 스펙 2단계).

## 가설
- `(account_id)`: 풀스캔은 피하지만 계좌의 모든 주문을 읽고 정렬해야 한다
- `(account_id, created_at)`: InnoDB가 PK를 뒤에 붙여 실제 순서가 `(account_id, created_at, id)`이므로,
  `ORDER BY created_at DESC, id DESC`를 역방향 스캔으로 처리하고 LIMIT 20에서 멈출 것이다
- `(account_id, status, created_at)`: 상태 필터 쿼리에는 유리하고, 필터 없는 쿼리에는 불리할 것이다
- 커버링: 테이블 접근이 사라지지만 인덱스 크기가 커질 것이다

## 조건
| 항목 | 값 |
|---|---|
| 환경 | 로컬 WSL2 Docker, MySQL 8.4.8 (메모리 1GB), order-server 컨테이너 (풀 30, 시뮬레이터 켬) |
| 데이터 | `orders` 약 127만 건 (시드 100만 + L1 부하 테스트 약 27만). 계좌 1은 약 4.6만 건 |
| 쿼리 | API와 동일: `WHERE account_id = ? [AND status = ?] AND created_at` 최근 30일 `ORDER BY created_at DESC, id DESC LIMIT 20` |
| 실행 계획 | 계좌 1, 계좌 1 + `status = 'CANCELED'`, 계좌 5000 |
| 부하 | k6 2.3.0, 50 VU 1분 (워밍업 10초 제외). 계좌 1 / 무작위 계좌 / 계좌 1 + 상태 필터 |
| 절차 | 후보마다 인덱스 추가 → `ANALYZE` → 측정 → 삭제 (`experiments/scripts/index-bench.sh`) |

## 결과

### 실행 계획 (계좌 1)
| 후보 | 크기 | `type` | `Extra` | 읽은 행 | 쿼리 시간 |
|---|---|---|---|---|---|
| 없음 | - | ALL | filesort | 1,270,000 | 1,032ms |
| `(account_id)` | 32MB | ref | Using where; Using filesort | 46,529 | 92ms |
| **`(account_id, created_at)`** | **43MB** | **range** | **Using index condition; Backward index scan** | **20** | **1.26ms** |
| `(account_id, status, created_at)` | 53MB | ref | Using index condition; Using filesort | 3,861 | 41ms |
| 커버링 | 105MB | range | Using index; Using filesort | 3,861 | 3.2ms |

PK(테이블) 크기는 132MB.

### 실행 계획 (계좌 1 + `status = 'CANCELED'`)
| 후보 | 방식 | 읽은 행 | 쿼리 시간 |
|---|---|---|---|
| 없음 | `idx_orders_status_id`로 CANCELED 전체(25만) 조회 후 필터 | 249,943 | 382ms |
| `(account_id)` | index merge (두 인덱스 교집합) 후 filesort | 11,588 | 119ms |
| `(account_id, created_at)` | 역방향 범위 스캔 후 상태 필터 | 154 | 1.06ms |
| `(account_id, status, created_at)` | 상태까지 포함한 역방향 범위 스캔 | 20 | 0.36ms |
| 커버링 | 커버링 범위 스캔 후 filesort | 3,861 | 1.5ms |

### k6 (50 VU 1분)
| 후보 | 계좌 1 처리량 / p95 | 무작위 계좌 처리량 / p95 | 계좌 1 + 상태 필터 처리량 / p95 |
|---|---|---|---|
| 없음 | 9.4/s / 6.83s | 9.0/s / 6.93s | 6.9/s / 9.05s |
| `(account_id)` | 33/s / 2.24s | 1,517/s / 75ms | 32/s / 3.02s |
| **`(account_id, created_at)`** | **1,201/s / 96ms** | **2,044/s / 55ms** | **1,286/s / 88ms** |
| `(account_id, status, created_at)` | 170/s / 636ms | 1,681/s / 68ms | 1,702/s / 68ms |
| 커버링 | 792/s / 123ms | 1,974/s / 54ms | 1,001/s / 96ms |

원본: `raw/20261019-Q1-explain-*.txt`, `raw/20261019-Q1-k6-*.txt`, `raw/20261019-Q1-summary.txt`

## 해석
- **`(account_id, created_at)`가 모든 쿼리 모양에서 안정적으로 빠르다.** 인덱스를 거꾸로 읽는 것만으로 정렬이 끝나 20행에서 멈춘다.
  개선 전 대비 계좌 1 처리량 약 127배(9.4 → 1,201/s), p95 약 71배(6.83s → 96ms), 쿼리 시간 약 800배(1,032ms → 1.26ms).
- **`(account_id)`는 주문이 많은 계좌에서 무너진다.** 무작위 계좌(주문 수십 건)는 빠르지만, 계좌 1은 4.6만 행을 모두 읽고 정렬한다.
  인덱스가 결과 수가 아니라 계좌 크기에 비례하는 비용을 남긴다.
- **`(account_id, status, created_at)`는 쿼리 모양에 따라 정반대다.** 상태 필터가 있으면 가장 빠르지만(1,702/s),
  없으면 `status`가 중간에 끼어 `created_at` 순서를 못 써 `(account_id, created_at)`보다 7배 느리다(170/s).
- **커버링 인덱스가 오히려 졌다.** 조회 컬럼을 모두 넣으면서 PK `id`가 맨 뒤로 밀려 `ORDER BY created_at DESC, id DESC`가 인덱스 순서와 어긋났다.
  테이블 접근은 사라졌지만 filesort가 다시 생겼고, 크기는 테이블의 80%(105MB)다. 정렬 조건이 있는 쿼리에서는 커버링보다 정렬 순서 일치가 우선이다.
- 상태 필터 쿼리도 `(account_id, created_at)`로 154행만 읽고 끝나, 전용 인덱스를 따로 둘 만큼 차이가 크지 않다 (1,286 vs 1,702/s).

## 조치
- 최종 인덱스: `idx_orders_account_created (account_id, created_at)`. `01-schema.sql`에 반영.
- 인덱스 추가의 쓰기 성능 영향은 다음 이슈에서 주문 접수 500 TPS 전후로 측정.
