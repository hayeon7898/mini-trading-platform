package mts.experiments;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * E3 컨슈머 그룹 lag 샘플러 (#43).
 * <pre>
 *   producer-tool lag --groups g1:topic1,g2:topic2 [--interval-ms 1000] [--out /out/lag.csv] [--bootstrap ...]
 * </pre>
 * 한 JVM에서 Admin 클라이언트를 재사용해 interval마다 (토픽 끝 오프셋 합 - 그룹 커밋 오프셋 합)을 기록한다.
 * kafka-consumer-groups.sh는 호출마다 JVM을 띄워 2~3초가 걸리고, kafka-exporter는 부하 중
 * 그룹 지표를 빠뜨려(시험 실행에서 -1 다수) 1초 해상도로 쓸 수 없었다.
 * 커밋 오프셋은 그룹에 멤버가 없어도(ClickHouse 큐 DETACH 중) 조회되므로 중단 구간의 lag도 기록된다.
 * 커밋 이력이 없는 파티션이 있으면 해당 그룹 값은 -1로 남긴다.
 * 종료: 프로세스 종료 신호(SIGTERM)까지 계속 기록한다.
 */
final class LagSampler {

    private LagSampler() {
    }

    static void run(Args a) throws Exception {
        String bootstrap = a.get("bootstrap", Args.DEFAULT_BOOTSTRAP);
        int intervalMs = a.getInt("interval-ms", 1000);
        String out = a.get("out", "/out/lag.csv");

        Map<String, String> groupToTopic = new LinkedHashMap<>();
        for (String pair : a.require("groups").split(",")) {
            String[] gt = pair.split(":", 2);
            groupToTopic.put(gt[0], gt[1]);
        }

        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

        try (Admin admin = Admin.create(p);
             PrintWriter w = new PrintWriter(new FileWriter(out, false), true)) {

            Map<String, List<TopicPartition>> partitions = new HashMap<>();
            for (String topic : new ArrayList<>(groupToTopic.values())) {
                int n = admin.describeTopics(List.of(topic)).allTopicNames().get()
                        .get(topic).partitions().size();
                List<TopicPartition> tps = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    tps.add(new TopicPartition(topic, i));
                }
                partitions.put(topic, tps);
            }

            w.println("ts_ms," + String.join(",", groupToTopic.keySet()));
            while (!Thread.currentThread().isInterrupted()) {
                long t0 = System.currentTimeMillis();
                StringBuilder line = new StringBuilder().append(t0);
                for (Map.Entry<String, String> e : groupToTopic.entrySet()) {
                    line.append(',').append(lag(admin, e.getKey(), partitions.get(e.getValue())));
                }
                w.println(line);
                long sleep = intervalMs - (System.currentTimeMillis() - t0);
                if (sleep > 0) {
                    TimeUnit.MILLISECONDS.sleep(sleep);
                }
            }
        }
    }

    private static long lag(Admin admin, String group, List<TopicPartition> tps) {
        try {
            Map<TopicPartition, OffsetSpec> req = new HashMap<>();
            tps.forEach(tp -> req.put(tp, OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin.listOffsets(req).all().get();
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();

            long sum = 0;
            for (TopicPartition tp : tps) {
                OffsetAndMetadata c = committed.get(tp);
                if (c == null) {
                    return -1;
                }
                sum += ends.get(tp).offset() - c.offset();
            }
            return sum;
        } catch (Exception ex) {
            return -1;
        }
    }
}
