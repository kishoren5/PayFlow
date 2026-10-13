import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const SCENARIO_MODE = __ENV.SCENARIO || 'steady';

export const options = {
  scenarios: {
    steady_flow: {
      executor: 'constant-arrival-rate',
      rate: 150,
      timeUnit: '1s',
      duration: '1m',
      preAllocatedVUs: 30,
      maxVUs: 150,
    },
    concurrency_race: {
      executor: 'per-vu-iterations',
      vus: 20,
      iterations: 5,
      startTime: '1m5s',
      maxDuration: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],
    http_req_duration: ['p(95)<300', 'p(99)<600'],
  },
};

export default function () {
  // Scenario 1: Unique transactions with distinct keys
  const uniqueKey = `k6-${__VU}-${__ITER}-${Date.now()}`;
  const payload = JSON.stringify({
    amount_minor: 2500,
    currency: 'INR',
    payment_method_token: 'pm_sandbox_success',
    capture_method: 'automatic',
    metadata: { source: 'k6_load_test' },
  });

  const res = http.post(`${BASE_URL}/v1/payments`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': uniqueKey,
      'X-Merchant-ID': 'merchant_k6_load',
    },
  });

  check(res, {
    'status is 200, 201, or 409': (r) => [200, 201, 409].includes(r.status),
    'response has payment id': (r) => r.json('id') !== undefined,
  });

  // Scenario 2: Immediate idempotent replay of same key
  const replayRes = http.post(`${BASE_URL}/v1/payments`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': uniqueKey,
      'X-Merchant-ID': 'merchant_k6_load',
    },
  });

  check(replayRes, {
    'replay status is 200': (r) => r.status === 200,
    'replay returns identical payment ID': (r) => r.json('id') === res.json('id'),
  });

  sleep(0.05);
}
