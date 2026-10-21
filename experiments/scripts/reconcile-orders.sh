#!/usr/bin/env bash
# MySQL 원장과 ClickHouse의 주문 최신 상태 대조 (#29). 저장소 루트에서 실행.
#   bash experiments/scripts/reconcile-orders.sh
#
# - 시드 주문(CDC 우회)은 제외한다 (기본 30~1,279,659, reconcile-executions.sh와 같은 기준).
# - 진행 중인 갱신과 섞이지 않게, 열린 주문이 0일 때 실행하는 것이 가장 정확하다.
# - 상태별 건수와 체결 수량 합을 비교하고, 상태가 다른 주문이 있으면 몇 건 보여 준다.
set -euo pipefail

SEED_MIN="${SEED_ORDER_MIN:-30}"
SEED_MAX="${SEED_ORDER_MAX:-1279659}"
WAIT="${WAIT:-10}"

mysql_q() {
  docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'
}
ch_q() {
  docker exec -i mts-clickhouse sh -c 'clickhouse-client --password "${CLICKHOUSE_PASSWORD:-}" -d mts'
}

open=$(mysql_q <<< "SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL');")
[[ "$open" != "0" ]] && echo "주의: 열린 주문 ${open}건. 갱신 중인 주문은 일시적으로 다를 수 있음"

MAX_ID=$(mysql_q <<< "SELECT COALESCE(MAX(id), 0) FROM orders;")
echo "기준 주문 id: ${MAX_ID} (적재 대기 ${WAIT}초)"
sleep "$WAIT"

FILTER="id <= ${MAX_ID} AND id NOT BETWEEN ${SEED_MIN} AND ${SEED_MAX}"

mysql_q <<< "SELECT status, COUNT(*), SUM(filled_qty) FROM orders WHERE ${FILTER} GROUP BY status ORDER BY status;" \
  > /tmp/recon-orders-my.tsv
ch_q <<< "SELECT status, count(), sum(filled_qty) FROM orders FINAL WHERE ${FILTER} GROUP BY status ORDER BY status FORMAT TSV;" \
  > /tmp/recon-orders-ch.tsv

printf '%-10s | %-24s | %s\n' "status" "MySQL (건수, 체결수량)" "ClickHouse FINAL"
join -a1 -a2 -e 0 -o 0,1.2,1.3,2.2,2.3 /tmp/recon-orders-my.tsv /tmp/recon-orders-ch.tsv \
  | awk '{printf "%-10s | %-24s | %s\n", $1, $2 " / " $3, $4 " / " $5}'

raw=$(ch_q <<< "SELECT count() FROM orders WHERE ${FILTER} FORMAT TSV;")
final=$(ch_q <<< "SELECT count() FROM orders FINAL WHERE ${FILTER} FORMAT TSV;")
echo "ClickHouse 머지 전 행: ${raw} (FINAL ${final}, 같은 주문의 이전 상태 $(( raw - final ))행)"

if cmp -s /tmp/recon-orders-my.tsv /tmp/recon-orders-ch.tsv; then
  echo "결과: 일치"
  exit 0
fi
echo "결과: 불일치. 상태가 다른 주문 예시 (최대 10건, 최근 주문부터):"
mysql_q <<< "SELECT id, status, filled_qty FROM orders WHERE ${FILTER} ORDER BY id DESC LIMIT 20000;" \
  | sort > /tmp/recon-orders-my-rows.tsv
ch_q <<< "SELECT id, status, filled_qty FROM orders FINAL WHERE ${FILTER} ORDER BY id DESC LIMIT 20000 FORMAT TSV;" \
  | sort > /tmp/recon-orders-ch-rows.tsv
comm -3 /tmp/recon-orders-my-rows.tsv /tmp/recon-orders-ch-rows.tsv | head -10
exit 1
