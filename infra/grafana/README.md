# Grafana 대시보드

접속: http://localhost:3000 (admin / `.env`의 `GRAFANA_ADMIN_PASSWORD`) → Dashboards → MTS → **MTS 파이프라인**

## 구성

| 파일 | 역할 |
|---|---|
| `provisioning/datasources/prometheus.yml` | Prometheus 데이터소스. uid를 `prometheus`로 고정 (대시보드 JSON이 이 값으로 참조) |
| `provisioning/dashboards/provider.yml` | `mts/` 폴더의 JSON을 대시보드로 등록. UI 저장 막음, 10초마다 파일 변경 반영 |
| `provisioning/dashboards/mts/mts-pipeline.json` | 대시보드 원본 |

볼륨을 두지 않는다. 컨테이너를 재생성하면 파일 기준으로 다시 뜨고, UI에서 만든 것은 사라진다.

## 패널

| 섹션 | 패널 | 쿼리 요약 | 관련 실험 |
|---|---|---|---|
| 상태 카드 | 살아 있는 브로커 / URP / 체결·주문 적재 lag / CDC 유입 / 주문 API p95 | 임계값에 따라 배경색 변경 | E1, E3 |
| Kafka · ClickHouse | 토픽 유입 속도 | `rate(kafka_topic_partition_current_offset{topic=~"mts\\..*"})` | E0, E4 |
| | 컨슈머 소비 속도 | `rate(kafka_consumergroup_current_offset{consumergroup=~"clickhouse-.*"})` | E3, E4 |
| | 컨슈머 그룹 lag | `kafka_consumergroup_lag_sum` | E3 |
| | 파티션별 lag (`$group` 변수) | `kafka_consumergroup_lag` 파티션별 누적 | E5 |
| | URP (토픽별) | `kafka_topic_partition_under_replicated_partition` | E1 |
| order-server | API 응답 시간 p95·p99 | `histogram_quantile` (`http_server_requests_seconds_bucket`) | L1 |
| | API 처리량 | 상태 코드별 `http_server_requests_seconds_count` | L1 |
| | DB 커넥션 풀 | `hikaricp_connections_{active,idle,pending,max}` | L1 |

PromQL 해석 시 주의점(5초 해상도, 묶음 커밋으로 인한 계단식 lag)은 [infra/prometheus/README.md](../prometheus/README.md) 참고.

order-server는 compose 밖에서 실행하므로, 띄우지 않으면 order-server 섹션과 "주문 API p95" 카드는 No data가 정상이다.

## 대시보드 수정 방법

UI의 저장 버튼은 막혀 있다 (파일이 원본). 수정 내용은 아래 순서로 반영한다.

1. UI에서 패널을 수정한다
2. 오른쪽 위 **Export → Export as JSON**
3. **"Export the dashboard to use in another instance"는 끈 채로** 복사한다.
   켜면 데이터소스가 `${DS_PROMETHEUS}` 변수로 바뀌어 파일로 넣었을 때 패널이 데이터를 찾지 못한다
4. `provisioning/dashboards/mts/mts-pipeline.json`을 덮어쓴다 → 10초 안에 반영 (재시작 불필요)

`provider.yml`이나 데이터소스 파일을 바꿨을 때는 기동 시에만 읽으므로 `docker compose restart grafana`가 필요하다.
