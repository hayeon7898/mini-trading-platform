#!/usr/bin/env bash
# 이미 떠 있는 ClickHouse에 init SQL의 Kafka 큐 설정을 반영한다 (#66).
#
# init SQL은 데이터 폴더가 비어 있을 때만 실행되므로, 큐 설정을 바꾸면 기존 환경에는 따로 반영해야 한다.
# Kafka 엔진은 ALTER ... MODIFY SETTING을 지원하지 않아(ClickHouse 26.8) 큐를 다시 만든다.
#   순서: MV 삭제 → 큐 삭제 → init SQL 다시 실행
# init SQL은 IF NOT EXISTS로 되어 있어, 지운 큐와 MV만 새 설정으로 생기고 대상 테이블과 데이터는 그대로 남는다.
# 컨슈머 그룹 이름이 같으므로 커밋된 오프셋부터 이어서 읽는다. MV가 생기는 순간 소비가 다시 시작된다.
#
# 사용법:
#   bash scripts/clickhouse-apply-queues.sh            # 체결, 주문 큐 모두
#   bash scripts/clickhouse-apply-queues.sh executions # 하나만
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INIT_DIR="$ROOT/infra/clickhouse/init"
CONTAINER="${CLICKHOUSE_CONTAINER:-mts-clickhouse}"

# shellcheck disable=SC2016  # 비밀번호 변수는 컨테이너 안에서 풀려야 한다
ch() { docker exec -i "$CONTAINER" sh -c 'clickhouse-client --user "${CLICKHOUSE_USER:-default}" --password "$CLICKHOUSE_PASSWORD" --multiquery "$@"' sh "$@"; }

# 이름: init SQL 파일, 큐, MV
targets=("executions:01-executions.sql:kafka_executions_queue:executions_mv"
         "orders:02-orders.sql:kafka_orders_queue:orders_mv")

only="${1:-}"
for t in "${targets[@]}"; do
  IFS=: read -r name file queue mv <<< "$t"
  [ -z "$only" ] || [ "$only" = "$name" ] || continue
  [ -f "$INIT_DIR/$file" ] || { echo "init SQL 없음: $INIT_DIR/$file" >&2; exit 1; }

  printf 'DROP TABLE IF EXISTS mts.%s;\nDROP TABLE IF EXISTS mts.%s;\n' "$mv" "$queue" | ch
  ch < "$INIT_DIR/$file"

  mode="$(ch -q "SELECT ifNull(nullIf(extract(engine_full, 'kafka_handle_error_mode = ''([^'']+)'''), ''), 'default')
    FROM system.tables WHERE database = 'mts' AND name = '${queue}'")"
  has_mv="$(ch -q "EXISTS TABLE mts.${mv}")"
  echo "[$name] $queue error_mode=$mode, $mv=$has_mv"
done
