#!/usr/bin/env bash
# MySQL 원장과 ClickHouse의 체결 대조 (#28). 저장소 루트에서 실행.
#   bash experiments/scripts/reconcile-executions.sh
#
# - 시드 체결은 sql_log_bin=0으로 CDC를 우회해 ClickHouse에 없으므로 제외한다.
#   시드 주문 id 범위(기본 30~1,279,659. 대량 INSERT의 AUTO_INCREMENT 빈틈으로 시드가 1,000,029보다 뒤까지 퍼짐)에 속한 주문의 체결을 뺀다.
# - 진행 중인 적재와 섞이지 않게, MySQL의 현재 최대 id를 기준점으로 잡고 WAIT초 기다린 뒤 그 id까지만 비교한다.
# - ClickHouse는 FINAL(중복 제거)과 일반 조회를 함께 보여 줘, 머지 전 중복 건수도 드러낸다.
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

MAX_ID=$(mysql_q <<< "SELECT COALESCE(MAX(id), 0) FROM executions;")
echo "기준 체결 id: ${MAX_ID} (적재 대기 ${WAIT}초)"
sleep "$WAIT"

FILTER="id <= ${MAX_ID} AND order_id NOT BETWEEN ${SEED_MIN} AND ${SEED_MAX}"

read -r my_rows my_sum <<< "$(mysql_q <<< "SELECT COUNT(*), COALESCE(SUM(quantity), 0) FROM executions WHERE ${FILTER};")"
read -r chf_rows chf_sum <<< "$(ch_q <<< "SELECT count(), sum(quantity) FROM executions FINAL WHERE ${FILTER} FORMAT TSV;")"
read -r chr_rows chr_sum <<< "$(ch_q <<< "SELECT count(), sum(quantity) FROM executions WHERE ${FILTER} FORMAT TSV;")"

printf '%-22s | %-10s | %s\n' "source" "rows" "sum(quantity)"
printf '%-22s | %-10s | %s\n' "MySQL" "$my_rows" "$my_sum"
printf '%-22s | %-10s | %s\n' "ClickHouse FINAL" "$chf_rows" "$chf_sum"
printf '%-22s | %-10s | %s\n' "ClickHouse (머지 전)" "$chr_rows" "$chr_sum"
echo "머지 전 중복 행: $(( chr_rows - chf_rows ))"

if [[ "$my_rows" == "$chf_rows" && "$my_sum" == "$chf_sum" ]]; then
  echo "결과: 일치"
  exit 0
fi

# 불일치: 주문 id를 1만 단위 구간으로 나눠 건수가 다른 구간만 보여 준다.
# 시드는 주문 단위로 정해지므로 주문 id로 나눠야 경계를 찾기 쉽다.
# 같은 구간 계산을 MySQL(DIV)과 ClickHouse(intDiv)에서 각자의 문법으로 한다.
echo "결과: 불일치. 체결 건수가 다른 주문 id 구간 (1만 단위):"
mysql_q <<< "SELECT order_id DIV 10000 * 10000 AS b, COUNT(*) FROM executions WHERE ${FILTER} GROUP BY b ORDER BY b;" \
  | sort > /tmp/recon-my.tsv
ch_q <<< "SELECT intDiv(order_id, 10000) * 10000 AS b, count() FROM executions FINAL WHERE ${FILTER} GROUP BY b ORDER BY b FORMAT TSV;" \
  | sort > /tmp/recon-ch.tsv
join -a1 -a2 -e 0 -o 0,1.2,2.2 /tmp/recon-my.tsv /tmp/recon-ch.tsv \
  | sort -n | awk '$2 != $3 {printf "  order_id %s~ : MySQL %s / ClickHouse %s\n", $1, $2, $3}' | head -30
exit 1