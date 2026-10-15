#!/usr/bin/env bash
# Debezium 전용 계정 생성.
# 비밀번호를 .env에서 받기 위해 .sql 대신 .sh로 작성.
# 주의: 실행 권한이 없으면 entrypoint가 source로 실행하므로 `set -u` 같은 셸 옵션을 바꾸지 않는다.

: "${DEBEZIUM_PASSWORD:?DEBEZIUM_PASSWORD 환경변수가 필요합니다}"

mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY '${DEBEZIUM_PASSWORD}';
-- 스냅샷(SELECT, RELOAD, SHOW DATABASES) + binlog 읽기(REPLICATION SLAVE, CLIENT)
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
SQL

echo "[init] debezium 계정 생성 완료"