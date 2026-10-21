#!/usr/bin/env bash
# Prometheus range query 결과를 CSV(time,series,value)로 출력한다.
# 사용법:
#   bash experiments/scripts/prom-range.sh '<PromQL>' <start> <end> [step] > out.csv
#   start/end: unix 초 또는 RFC3339 (예: "$(date -d '-10 min' +%s)" "$(date +%s)")
set -euo pipefail

PROM_URL="${PROM_URL:-http://localhost:9090}"

if [ "$#" -lt 3 ]; then
  echo "usage: $0 '<promql>' <start> <end> [step=5s]" >&2
  exit 1
fi

query="$1"
start="$2"
end="$3"
step="${4:-5s}"

echo "time,series,value"
curl -fsG "$PROM_URL/api/v1/query_range" \
  --data-urlencode "query=$query" \
  --data-urlencode "start=$start" \
  --data-urlencode "end=$end" \
  --data-urlencode "step=$step" |
  jq -r '
    .data.result[] as $s
    | ($s.metric | del(.__name__) | to_entries
        | map("\(.key)=\(.value)") | join(";")) as $labels
    | $s.values[]
    | [(.[0] | floor | todate), $labels, .[1]]
    | @csv'
