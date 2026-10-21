#!/usr/bin/env bash
# DB 커밋 → ClickHouse 적재 end-to-end 지연 (#30). 저장소 루트에서 실행.
#   bash experiments/scripts/e2e-latency-bench.sh
#   FLUSH_LIST="7500 1000 500" RATE=500 DURATION=2m bash experiments/scripts/e2e-latency-bench.sh
#
# kafka_flush_interval_ms마다:
#   1) 체결 큐·MV를 그 값으로 다시 만든다 (executions 테이블과 컨슈머 오프셋은 유지)
#   2) 열린 주문이 0이 될 때까지 기다린다 (이전 실행의 체결이 섞이지 않게)
#   3) 주문 접수 부하를 주고, 그 사이 커밋된 체결의 지연 분위수를 구간별로 집계한다
# 전제: mts-order-server가 시뮬레이터를 켠 상태로 같은 Docker 네트워크에서 실행 중
set -euo pipefail

read -r -a FLUSHES <<< "${FLUSH_LIST:-7500 1000 500}"
RATE="${RATE:-500}"
DURATION="${DURATION:-2m}"
INIT=infra/clickhouse/init/01-executions.sql
DATE=$(date +%Y%m%d)
OUT="${OUT:-experiments/results/${DATE}-E0-e2e-latency}/raw"
NET=$(docker inspect mts-mysql -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
mkdir -p "$OUT"

mysql_q() {
  docker exec -i mts-mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot trading -N'
}
ch_q() {
  docker exec -i mts-clickhouse clickhouse-client -d mts "$@"
}

wait_idle() {
  local open
  while true; do
    open=$(mysql_q <<< "SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL');")
    [[ "$open" == "0" ]] && return
    echo "  열린 주문 ${open}건 대기"
    sleep 10
  done
}

recreate_queue() { # $1=flush ms
  ch_q --multiquery <<< "DROP VIEW IF EXISTS executions_mv; DROP TABLE IF EXISTS kafka_executions_queue;"
  sed "s/kafka_flush_interval_ms = [0-9]*/kafka_flush_interval_ms = $1/" "$INIT" | ch_q --multiquery
  ch_q -q "SELECT engine_full LIKE '%kafka_flush_interval_ms = $1%' FROM system.tables
           WHERE database = 'mts' AND name = 'kafka_executions_queue'" | grep -q 1
}

SUMMARY="$OUT/${DATE}-E0-summary.txt"
printf '%-6s | %-8s | %-24s | %-24s | %s\n' "flush" "rows" "total p50/p95/p99 (ms)" \
  "DB→Kafka p50/p95/p99" "Kafka→CH p50/p95/p99" | tee "$SUMMARY"

for f in "${FLUSHES[@]}"; do
  echo "== kafka_flush_interval_ms = $f"
  recreate_queue "$f"
  wait_idle
  sleep 15

  start=$(date -u +'%Y-%m-%d %H:%M:%S')
  docker run --rm -i --network "$NET" -v "$PWD/load-test:/scripts" grafana/k6:2.3.0 run --quiet \
    -e BASE_URL=http://mts-order-server:8080 -e RATE="$RATE" -e DURATION="$DURATION" /scripts/orders.js \
    > "$OUT/${DATE}-E0-k6-flush${f}.txt" 2>&1 || true
  end=$(date -u +'%Y-%m-%d %H:%M:%S')
  wait_idle
  sleep $(( f / 1000 + 15 ))

  row=$(ch_q -q "
    SELECT
      count(),
      arrayStringConcat(arrayMap(x -> toString(round(x)), quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', source_ts, ingested_at))), '/'),
      arrayStringConcat(arrayMap(x -> toString(round(x)), quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', source_ts, kafka_ts))), '/'),
      arrayStringConcat(arrayMap(x -> toString(round(x)), quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', kafka_ts, ingested_at))), '/')
    FROM executions
    WHERE source_ts >= '$start' AND source_ts < '$end' AND kafka_ts > toDateTime64(0, 3, 'UTC')
    FORMAT TSV")
  read -r rows total dbk kch <<< "$row"
  printf '%-6s | %-8s | %-24s | %-24s | %s\n' "$f" "$rows" "$total" "$dbk" "$kch" | tee -a "$SUMMARY"
done
echo "원본: $OUT"
