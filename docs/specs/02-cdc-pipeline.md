# 02. CDC 파이프라인 (MySQL → Debezium → Kafka)

## 목적

원장 DB의 모든 변경(INSERT/UPDATE)을 **누락 없이, 커밋 순서대로** Kafka에 발행한다.

## MySQL 설정

`docker-compose.yml`의 `mysql.command`에 실행 옵션으로 지정한다. (Windows 마운트 시 `my.cnf`가 world-writable로 무시되는 문제 때문에 설정 파일을 쓰지 않음)

- `--binlog-format=ROW`, `--binlog-row-image=FULL`, `--gtid-mode=ON`, `--enforce-gtid-consistency=ON`, `--server-id=1`
- Debezium 전용 계정: `infra/mysql/init/02-debezium-user.sh` (`SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT`)

## Kafka 클러스터

- 3 브로커, KRaft combined mode (브로커가 컨트롤러 역할 겸임)
- 기본값: `default.replication.factor=3`, `min.insync.replicas=2`, `num.partitions=6`
- `auto.create.topics.enable=false` 로 두고 토픽은 스크립트로 명시 생성 → 파티션·복제 설정을 의도대로 통제

## Debezium 커넥터 설정 (초안)

```json
{
  "name": "mts-trading-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "${env:DEBEZIUM_PASSWORD}",
    "database.server.id": "5400",
    "topic.prefix": "mts",
    "database.include.list": "trading",
    "table.include.list": "trading.orders,trading.executions",
    "schema.history.internal.kafka.bootstrap.servers": "kafka-1:9092",
    "schema.history.internal.kafka.topic": "schema-history.mts",
    "snapshot.mode": "initial",
    "decimal.handling.mode": "string",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,source.ts_ms"
  }
}
```

설정 선택 이유 (README에 기록):

- `decimal.handling.mode=string`: 기본값이면 DECIMAL이 바이트 인코딩되어 다운스트림에서 파싱이 어렵다. 금액 정밀도를 보존하면서 읽기 쉬운 형태로.
- `ExtractNewRecordState`: Debezium 기본 envelope(before/after)를 평탄화해 ClickHouse가 바로 읽게 함. 대신 변경 전 값은 잃는다는 트레이드오프.
- `add.fields=op,source.ts_ms`: 연산 종류와 DB 커밋 시각을 남겨 end-to-end 지연 측정에 사용.

## 확인해야 할 함정

- **토픽 선생성**: 토픽 자동 생성이 꺼져 있으므로 커넥터 등록 **전에** `./scripts/create-topics.sh` 실행. 안 하면 커넥터가 토픽을 못 찾아 실패한다.
- **타임스탬프 포맷**: DATETIME 컬럼이 어떤 형태(epoch millis 등)로 나오는지 실제 메시지를 보고 ClickHouse 컬럼 타입을 맞춘다.
- **스냅샷 vs 스트리밍**: 커넥터 최초 기동 시 기존 1,000만 건을 스냅샷으로 읽는다. 시간이 오래 걸리면 시드 데이터 적재 전에 커넥터를 등록하거나 스냅샷 모드를 조정한다.
- **메시지 키**: 기본은 PK. 같은 계좌의 주문 순서가 필요해지면 키 재설정이 필요하다 (2단계 과제).

## 검증

- `kcat` 또는 `kafka-console-consumer`로 토픽 메시지 형태 확인
- 일정 시간 부하 후 MySQL 행 수와 토픽 메시지 수 비교 (UPDATE 포함이므로 orders는 이벤트 수 ≥ 행 수)

## 완료 기준

- [ ] 주문 접수·체결 시 1초 이내에 토픽에 메시지 도착
- [ ] 커넥터 재시작 후 이어서 발행되며 누락 없음 (중복은 허용, 04 스펙 E2에서 측정)
- [ ] 토픽 파티션·복제 설정이 의도대로인지 `kafka-topics --describe`로 확인