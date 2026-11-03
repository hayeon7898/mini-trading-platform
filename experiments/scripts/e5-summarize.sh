#!/usr/bin/env bash
# E5 실행 결과 폴더 하나를 요약한다 (#64, 07 스펙).
# e5-run.sh가 끝에서 호출하고, 이미 끝난 실행도 원본 파일로 다시 요약할 수 있다.
#
# 사용법: bash experiments/scripts/e5-summarize.sh <실행 결과 폴더>
#   필요한 파일: attach.txt offsets-detach.txt offsets-attach.txt members.csv lag-partitions.csv lag.csv
#   만드는 파일: partition-summary.csv summary.txt
#   환경변수: RESOLVE_LAG(10000) PART_RESOLVE_LAG(2000)
#
# - backlog는 큐마다 자기 ATTACH 직전 마지막 lag 샘플이다.
# - 해소 시간은 큐마다 자기 ATTACH 직전 시각을 기준으로 잰다.
# - 담당 컨슈머는 ATTACH 뒤 배정이 바뀐 순서대로 적는다 (예: c2>c0).
#   ClickHouse 컨슈머가 몇 초 간격으로 그룹에 들어오면 처음 들어온 하나가 전부를 받았다가 재배정된다.
#   그래서 컨슈머별 처리량은 계산하지 않고, 배정 변화 시각을 함께 남긴다.
set -euo pipefail

OUT="${1:?실행 결과 폴더가 필요함}"
RESOLVE_LAG="${RESOLVE_LAG:-10000}"
PART_RESOLVE_LAG="${PART_RESOLVE_LAG:-2000}"

for f in attach.txt offsets-detach.txt offsets-attach.txt members.csv lag-partitions.csv lag.csv; do
  [ -f "$OUT/$f" ] || { echo "파일 없음: $OUT/$f" >&2; exit 1; }
done

# --- 파티션별 요약 ------------------------------------------------------------
awk -F'[:,]' -v thr="$PART_RESOLVE_LAG" '
  BEGIN {
    topic["clickhouse-executions"] = "mts.trading.executions"
    topic["clickhouse-orders"] = "mts.trading.orders"
  }
  FILENAME ~ /\/attach\.txt$/ { at[$1] = $2; next }   # offsets-attach.txt와 구분
  FILENAME ~ /offsets-detach/ { d[$1 "," $2] = $3; next }
  FILENAME ~ /offsets-attach/ { inflow[$1 "," $2] = $3 - d[$1 "," $2]; next }
  FILENAME ~ /members/ {
    if (FNR == 1 || $1 < at[$2] || $5 == "-") next
    k = $2 "," $3; n = split($5, z, "-"); cid = "c" z[n]
    if (!(k in last) || last[k] != cid) {
      # mawk는 c[k] = ... 의 왼쪽을 먼저 만들므로 값을 따로 계산해 넣는다
      seq = (k in last) ? c[k] ">" cid : cid
      c[k] = seq; last[k] = cid
    }
    next
  }
  FILENAME ~ /lag-partitions/ {
    if (FNR == 1) next
    k = $2 "," $3
    if (!(k in seen)) { seen[k] = 1; order[++cnt] = k }
    if ($1 < at[$2]) { b[k] = $6; next }
    if (!(k in r) && $6 >= 0 && $6 <= thr) r[k] = sprintf("%.1f", ($1 - at[$2]) / 1000)
    next
  }
  END {
    print "group,partition,consumers,inflow_stop,backlog_before_attach,resolve_sec"
    for (i = 1; i <= cnt; i++) {
      k = order[i]; split(k, a, ",")
      print k "," ((k in c) ? c[k] : "-") "," inflow[topic[a[1]] "," a[2]] "," b[k] "," ((k in r) ? r[k] : "-")
    }
  }' "$OUT/attach.txt" "$OUT/offsets-detach.txt" "$OUT/offsets-attach.txt" "$OUT/members.csv" "$OUT/lag-partitions.csv" \
  > "$OUT/partition-summary.csv"

# --- 요약 ------------------------------------------------------------------
{
  echo "$(basename "$(dirname "$OUT")")/$(basename "$OUT") part_resolve_lag=$PART_RESOLVE_LAG"

  # 토픽 해소: 그룹 lag 합계가 RESOLVE_LAG 이하가 된 첫 시점
  awk -F, -v thr="$RESOLVE_LAG" '
    FNR == NR { at[$1] = $2; next }
    FNR == 1 { next }
    {
      ae = at["clickhouse-executions"]; ao = at["clickhouse-orders"]
      if (re == "" && $1 >= ae && $2 >= 0 && $2 <= thr) re = sprintf("%.1f", ($1 - ae) / 1000)
      if (ro == "" && $1 >= ao && $3 >= 0 && $3 <= thr) ro = sprintf("%.1f", ($1 - ao) / 1000)
    }
    END { printf "토픽 해소 (lag 합계 <= %d, 체결/주문): %s / %s초\n", thr, (re == "" ? "-" : re), (ro == "" ? "-" : ro) }
  ' "$OUT/attach.txt" "$OUT/lag.csv"

  # 그룹별 쏠림과 해소 시간 분포
  awk -F, '
    NR == 1 { next }
    {
      g = $1; n[g]++; si[g] += $4; sb[g] += $5
      if ($4 > mi[g]) mi[g] = $4
      if ($5 > mb[g]) mb[g] = $5
      if ($6 == "-") un[g]++; else { m[g]++; v[g, m[g]] = $6 + 0 }
    }
    END {
      for (g in n) {
        # 해소 시간 오름차순 정렬 (mawk에는 asort가 없어 삽입 정렬)
        for (i = 2; i <= m[g]; i++) {
          x = v[g, i]; j = i - 1
          while (j >= 1 && v[g, j] > x) { v[g, j + 1] = v[g, j]; j-- }
          v[g, j + 1] = x
        }
        fast = v[g, 1]; slow = v[g, m[g]]
        # 가장 늦은 파티션을 뺀 나머지의 중앙값
        k = m[g] - 1; med = 0
        if (k >= 1) med = (k % 2 == 1) ? v[g, (k + 1) / 2] : (v[g, k / 2] + v[g, k / 2 + 1]) / 2
        printf "[%s] 유입 최대/평균 %.2f, backlog 최대/평균 %.2f\n", g,
          (si[g] > 0 ? mi[g] / (si[g] / n[g]) : 0), (sb[g] > 0 ? mb[g] / (sb[g] / n[g]) : 0)
        printf "[%s] 파티션 해소 가장 빠름 %.1f초, 가장 늦음 %.1f초, 나머지 중앙값 %.1f초, 가장 늦음 ÷ 나머지 중앙값 %.2f, 미해소 %d\n",
          g, fast, slow, med, (med > 0 ? slow / med : 0), un[g] + 0
      }
    }' "$OUT/partition-summary.csv" | sort

  # 배정 변화: ATTACH 뒤 시각별로 그룹에 들어온 컨슈머 수
  awk -F, '
    FNR == NR { at[$1] = $2; next }
    FNR == 1 || $5 == "-" || $1 < at[$2] { next }
    {
      k = $2 SUBSEP $1; n = split($5, z, "-"); cid = "c" z[n]
      if (!(k in cnt)) { cnt[k] = 0; order[++o] = k }
      if (!((k, cid) in s)) { s[k, cid] = 1; cnt[k]++ }
    }
    END {
      for (i = 1; i <= o; i++) {
        split(order[i], a, SUBSEP)
        printf "[%s] 배정 +%.1f초: 컨슈머 %d개\n", a[1], (a[2] - at[a[1]]) / 1000, cnt[order[i]]
      }
    }' "$OUT/attach.txt" "$OUT/members.csv"

  [ -f "$OUT/reconcile.txt" ] && cat "$OUT/reconcile.txt"
} | tee "$OUT/summary.txt"
