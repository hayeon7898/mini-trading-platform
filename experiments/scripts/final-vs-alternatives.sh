#!/usr/bin/env bash
# FINAL vs 대안 집계 쿼리의 실행 시간과 결과 비교 (#30). 저장소 루트에서 실행.
#   bash experiments/scripts/final-vs-alternatives.sh
#
# 같은 집계를 방식별로 RUNS번 실행해 중앙값(ms)을 비교하고, 결과가 같은지 확인한다.
set -euo pipefail

RUNS="${RUNS:-5}"
DATE=$(date +%Y%m%d)
OUT="${OUT:-experiments/results/${DATE}-E0-e2e-latency}/raw"
mkdir -p "$OUT"

ch() { docker exec -i mts-clickhouse clickhouse-client -d mts "$@"; }

# 이름|쿼리
QUERIES=(
  "exec_final|SELECT count(), sum(quantity) FROM executions FINAL"
  "exec_argmax|SELECT count(), sum(q) FROM (SELECT id, argMax(quantity, ingested_at) AS q FROM executions GROUP BY id)"
  "exec_limit_by|SELECT count(), sum(quantity) FROM (SELECT id, quantity FROM executions ORDER BY id, ingested_at DESC LIMIT 1 BY id)"
  "exec_uniq|SELECT uniqExact(id) FROM executions"
  "exec_raw|SELECT count(), sum(quantity) FROM executions"
  "orders_final|SELECT status, count(), sum(filled_qty) FROM orders FINAL GROUP BY status ORDER BY status"
  "orders_argmax|SELECT s, count(), sum(f) FROM (SELECT id, argMax(status, version) AS s, argMax(filled_qty, version) AS f FROM orders GROUP BY id) GROUP BY s ORDER BY s"
  "orders_raw|SELECT status, count(), sum(filled_qty) FROM orders GROUP BY status ORDER BY status"
)

RESULT="$OUT/${DATE}-E0-final-vs-alternatives.txt"
{
  echo "-- 머지 전 행 수 / 고유 id 수"
  ch -q "SELECT 'executions', count(), uniqExact(id) FROM executions FORMAT TSV"
  ch -q "SELECT 'orders', count(), uniqExact(id) FROM orders FORMAT TSV"
  echo
  printf '%-15s | %-10s | %s\n' "query" "median_ms" "result"
} | tee "$RESULT"

for entry in "${QUERIES[@]}"; do
  name="${entry%%|*}"; q="${entry#*|}"
  times=()
  for (( i = 0; i < RUNS; i++ )); do
    # 캐시 효과를 줄이려고 쿼리 결과 캐시를 끄고, 서버가 잰 실행 시간(초)을 받는다
    t=$(ch --time --use_query_cache=0 -q "$q" 2>&1 >/dev/null | tail -1)
    times+=("$t")
  done
  median=$(printf '%s\n' "${times[@]}" | sort -n | awk '{t[NR]=$1} END {printf "%.0f", t[int((NR+1)/2)] * 1000}')
  result=$(ch -q "$q FORMAT TSV" | tr '\t\n' ', ' | sed 's/, $//')
  printf '%-15s | %-10s | %s\n' "$name" "$median" "$result" | tee -a "$RESULT"
done
echo "원본: $RESULT"
