// 계좌별 주문 내역 조회 부하 (05 스펙). 고정 VU 수로 일정 시간 반복 호출한다.
//   ACCOUNT=1       → 주문이 많은 계좌 (약 4.6만 건)
//   ACCOUNT=random  → 계좌 1~10,000 무작위 (일반 계좌)
//   STATUS=CANCELED → 상태 필터 쿼리
//   VUS=50 DURATION=1m (기본값, 스펙 기준)
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://mts-order-server:8080';
const ACCOUNT = __ENV.ACCOUNT || 'random';
const STATUS = __ENV.STATUS || '';

export const options = {
  vus: Number(__ENV.VUS || 50),
  duration: __ENV.DURATION || '1m',
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const id = ACCOUNT === 'random' ? 1 + Math.floor(Math.random() * 10000) : ACCOUNT;
  const status = STATUS ? `&status=${STATUS}` : '';
  const res = http.get(`${BASE_URL}/accounts/${id}/orders?size=20${status}`, {
    tags: { name: 'GET /accounts/{id}/orders' },
  });
  check(res, { 'status is 200': (r) => r.status === 200 });
}
