# Prometheus 수집 대상과 쿼리

## 수집 대상

| job | 대상 | 지표 |
|---|---|---|
| order-server | `mts-order-server:8080/actuator/prometheus` | HTTP 지연, HikariCP, JVM |
| kafka-exporter | `kafka-exporter:9308` | 컨슈머 그룹 lag, 파티션 오프셋, ISR·URP, 브로커 수 |

설정 변경 후 반영: `docker kill -s HUP mts-prometheus` (폴더 마운트라 재시작 없이 다시 읽음)

## 04 실험용 쿼리

| 용도 | PromQL |
|---|---|
| 컨슈머 그룹별 lag | `kafka_consumergroup_lag_sum{consumergroup=~"clickhouse-.*"}` |
| 파티션별 lag | `kafka_consumergroup_lag{consumergroup="clickhouse-executions"}` |
| Under-replicated partitions (전체) | `sum(kafka_topic_partition_under_replicated_partition)` |
| URP (토픽별) | `sum by (topic) (kafka_topic_partition_under_replicated_partition)` |
| 파티션별 ISR 수 | `kafka_topic_partition_in_sync_replica{topic="<토픽>"}` |
| 파티션 리더 브로커 | `kafka_topic_partition_leader{topic="<토픽>"}` |
| 토픽 유입 속도 (msg/s) | `sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"mts\\..*"}[30s]))` |
| 컨슈머 소비 속도 (msg/s) | `sum by (consumergroup, topic) (rate(kafka_consumergroup_current_offset[30s]))` |
| 살아 있는 브로커 수 | `kafka_brokers` |

## 해석 시 주의

| 항목 | 내용 |
|---|---|
| 해상도 | kafka-exporter는 수집 시점의 오프셋 스냅샷이라 5초 간격이 최소 해상도. 리더 재선출처럼 1초 미만 이벤트는 브로커 로그·프로듀서 로그로 측정 |
| lag 기준 | ClickHouse Kafka 엔진이 **커밋한** 오프셋 기준. 묶음 단위 커밋이라 실제보다 계단식으로 보임 |
| `rate` 창 | 30초 창은 반응이 빠른 대신 출렁임이 큼. 그래프용은 `[1m]` 권장 |

## CSV 추출

```bash
bash experiments/scripts/prom-range.sh \
  'kafka_consumergroup_lag_sum{consumergroup="clickhouse-executions"}' \
  "$(date -d '-10 min' +%s)" "$(date +%s)" 5s > lag.csv
```
