# 05. 주문 내역 조회 쿼리 개선 (EXPLAIN)

## 목적

1,000만 건 규모 `orders` 테이블에서 계좌별 주문 내역 조회가 느린 문제를 실행 계획 분석으로 개선한다. DE·BE 공통으로 쓸 수 있는 에피소드.

## 대상 API

`GET /accounts/{id}/orders?from=&to=&status=&size=20`

```sql
SELECT id, symbol, side, price, quantity, filled_qty, status, created_at
FROM orders
WHERE account_id = ? AND created_at BETWEEN ? AND ?
  AND status IN (?)
ORDER BY created_at DESC
LIMIT 20 OFFSET ?;
```

## 단계

### 1) 개선 전 측정

- 인덱스 없음(PK만) 상태에서 `EXPLAIN`, `EXPLAIN ANALYZE` 실행
- 확인할 항목: `type`(ALL = 풀스캔), `rows`, `Extra`(Using filesort, Using where)
- k6로 해당 API를 50 VU로 1분 호출 → p95, 처리량 기록

### 2) 인덱스 설계와 비교

| 후보 | 예상 |
|---|---|
| `(account_id)` | 풀스캔은 피하지만 정렬에 filesort 발생 |
| `(account_id, created_at)` | 범위 조회 + 정렬을 인덱스로 해결 |
| `(account_id, status, created_at)` | status 필터 선택도에 따라 유리/불리 |
| 위 + 조회 컬럼 포함(커버링) | 테이블 접근 제거, 대신 인덱스 크기·쓰기 비용 증가 |

각 후보마다 `EXPLAIN` 결과와 k6 수치를 표로 남긴다.

### 3) 페이지네이션

깊은 페이지(`OFFSET 100000`)에서 느려지는 현상을 측정하고, 커서 기반(`WHERE created_at < :last_created_at`, 동률 처리를 위해 `id` 병행) 방식으로 바꿔 비교한다.

## 트레이드오프로 기록할 것

- 인덱스 추가가 **쓰기 성능**(주문 접수 TPS)에 미치는 영향: 01 스펙의 k6 주문 부하를 인덱스 추가 전후로 다시 측정
- CDC 관점: 인덱스는 binlog에 영향 없음 → 파이프라인과 무관하다는 점도 확인

## 완료 기준

- [ ] 개선 전/후 `EXPLAIN` 결과 캡처
- [ ] p95 응답시간 개선 수치
- [ ] 쓰기 성능 영향 수치
- [ ] OFFSET vs 커서 페이지네이션 비교
