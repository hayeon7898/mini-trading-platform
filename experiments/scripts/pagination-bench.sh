#!/usr/bin/env bash
# OFFSET vs 커서 페이지 이동 비교 (#24). 저장소 루트에서 실행.
#   bash experiments/scripts/pagination-bench.sh
#
# 계좌 1의 최근 1년 주문에서 깊이(앞에 건너뛸 행 수)별로
#   OFFSET: page = 깊이 / 20
#   커서:   깊이 바로 앞 행의 (created_at, id)로 만든 커서
# 를 각각 API로 10번씩 호출해 응답 시간 중앙값을 비교하고, 같은 주문이 오는지 확인한다.
set -euo pipefail

ACCOUNT="${ACCOUNT:-1}"
SIZE=20
read -r -a DEPTHS <<< "${DEPTH_LIST:-0 1000 10000 40000}"
RUNS=10
BASE="${BASE:-http://localhost:8080}"
FROM=$(date -u -d '400 days ago' +%Y-%m-%dT00:00:00Z)
DATE=$(date +%Y%m%d)
OUT="${OUT:-experiments/results/${DATE}-P1-pagination}/raw"
mkdir -p "$OUT"

sql() {
  docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'
}

# 깊이 d 바로 앞 행(d번째 행)의 커서
cursor_at() {
  local d=$1 row
  row=$(sql <<< "SET time_zone = '+00:00';
    SELECT CONCAT(ROUND(UNIX_TIMESTAMP(created_at) * 1000), ':', id) FROM orders
    WHERE account_id = $ACCOUNT AND created_at >= '${FROM/T/ }'
    ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET $(( d - 1 ));")
  printf '%s' "$row" | base64 | tr '+/' '-_' | tr -d '=\n'
}

median_ms() { # $1=URL
  local i times=()
  for i in 1 2; do curl -s -o /dev/null "$1"; done
  for (( i = 0; i < RUNS; i++ )); do
    times+=("$(curl -s -o /dev/null -w '%{time_total}' "$1")")
  done
  printf '%s\n' "${times[@]}" | sort -n | awk '{t[NR]=$1} END {printf "%.1f", t[int((NR+1)/2)] * 1000}'
}

first_id() { curl -s "$1" | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2; }

RESULT="$OUT/${DATE}-P1-summary.txt"
printf '%-8s | %-12s | %-12s | %s\n' "depth" "offset(ms)" "cursor(ms)" "same_first_id" | tee "$RESULT"

for d in "${DEPTHS[@]}"; do
  url="$BASE/accounts/$ACCOUNT/orders?from=$FROM&size=$SIZE"
  offset_url="$url&page=$(( d / SIZE ))"
  if (( d == 0 )); then
    cursor_url="$url"
  else
    cursor_url="$url&cursor=$(cursor_at "$d")"
  fi

  o=$(median_ms "$offset_url")
  c=$(median_ms "$cursor_url")
  same=no
  [[ "$(first_id "$offset_url")" == "$(first_id "$cursor_url")" ]] && same=yes
  printf '%-8s | %-12s | %-12s | %s\n' "$d" "$o" "$c" "$same" | tee -a "$RESULT"
done

# 가장 깊은 지점의 실행 계획
deep=${DEPTHS[-1]}
{
  echo "-- OFFSET $deep"
  sql <<< "EXPLAIN ANALYZE SELECT * FROM orders WHERE account_id = $ACCOUNT AND created_at >= '${FROM/T/ }'
           ORDER BY created_at DESC, id DESC LIMIT $SIZE OFFSET $deep\G"
} > "$OUT/${DATE}-P1-explain-offset.txt"
echo "원본: $OUT"
