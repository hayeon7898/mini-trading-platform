// 주문 접수 부하. TPS와 시간을 환경변수로 받아 단계별로 따로 실행한다.
//   RATE=100 DURATION=1m  → 워밍업
//   RATE=500 DURATION=5m  → 완료 기준 (에러율 1% 미만)
//   RATE=1000 DURATION=2m → 한계 확인
//
// - constant-arrival-rate: 응답이 느려져도 초당 요청 수를 유지한다 (VU 수 고정 방식과 달리
//   서버가 느려지면 요청도 줄어드는 착시가 없다). VU가 모자라면 dropped_iterations로 드러난다.
// - 계좌 1~10,000 균등, 종목은 상위 10개(S0001~S0010)에 50%를 몰아 핫 종목을 만든다.
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const RATE = Number(__ENV.RATE || 100);
const DURATION = __ENV.DURATION || '1m';

export const options = {
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(20, RATE / 5),
      maxVUs: RATE * 2,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

function randInt(min, max) {
  return min + Math.floor(Math.random() * (max - min + 1));
}

function symbol() {
  const n = Math.random() < 0.5 ? randInt(1, 10) : randInt(11, 100);
  return 'S' + String(n).padStart(4, '0');
}

export default function () {
  const s = symbol();
  const base = 1000 * Number(s.slice(1)); // 시드의 기준가 규칙과 동일
  const body = JSON.stringify({
    accountId: randInt(1, 10000),
    symbol: s,
    side: Math.random() < 0.5 ? 'BUY' : 'SELL',
    price: base,
    quantity: randInt(1, 100),
  });
  const res = http.post(`${BASE_URL}/orders`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /orders' },
  });
  check(res, { 'status is 201': (r) => r.status === 201 });
}
