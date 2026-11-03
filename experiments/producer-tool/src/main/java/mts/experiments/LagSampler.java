package mts.experiments;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * 컨슈머 그룹 lag 샘플러 (E3 #43, E5 #64).
 * <pre>
 *   producer-tool lag --groups g1:topic1,g2:topic2 [--interval-ms 1000] [--out /out/lag.csv] [--bootstrap ...]
 *                     [--partitions-out /out/lag-partitions.csv] [--members-out /out/members.csv]
 * </pre>
 * 한 JVM에서 Admin 클라이언트를 재사용해 interval마다 (토픽 끝 오프셋 합 - 그룹 커밋 오프셋 합)을 기록한다.
 * kafka-consumer-groups.sh는 호출마다 JVM을 띄워 2~3초가 걸리고, kafka-exporter는 부하 중
 * 그룹 지표를 빠뜨려(시험 실행에서 -1 다수) 1초 해상도로 쓸 수 없었다.
 * 커밋 오프셋은 그룹에 멤버가 없어도(ClickHouse 큐 DETACH 중) 조회되므로 중단 구간의 lag도 기록된다.
 * 커밋 이력이 없는 파티션이 있으면 해당 그룹 값은 -1로 남긴다.
 *
 * 선택 출력 (E5):
 * <ul>
 *   <li>--partitions-out : 같은 시점의 파티션별 끝 오프셋, 커밋 오프셋, lag (커밋 이력이 없으면 -1)</li>
 *   <li>--members-out    : 파티션별 담당 컨슈머. 배정이 바뀔 때만 그 시점의 전체 배정을 남긴다
 *                          (그룹에 멤버가 없으면 "-")</li>
 * </ul>
 * 선택 출력을 주지 않으면 lag.csv만 이전과 같은 형식으로 쓴다.
 * 종료: 프로세스 종료 신호(SIGTERM)까지 계속 기록한다.
 */
final class LagSampler {

    private LagSampler() {
    }

    /** 그룹 하나의 한 시점 값. 조회에 실패하면 perPartition이 비어 있고 total은 -1이다. */
    private record GroupSample(long total, Map<TopicPartition, long[]> perPartition) {
        static final GroupSample FAILED = new GroupSample(-1, Map.of());
    }

    static void run(Args a) throws Exception {
        String bootstrap = a.get("bootstrap", Args.DEFAULT_BOOTSTRAP);
        int intervalMs = a.getInt("interval-ms", 1000);
        String out = a.get("out", "/out/lag.csv");
        String partitionsOut = a.get("partitions-out", "");
        String membersOut = a.get("members-out", "");

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
             PrintWriter w = new PrintWriter(new FileWriter(out, false), true);
             PrintWriter pw = optionalWriter(partitionsOut);
             PrintWriter mw = optionalWriter(membersOut)) {

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
            if (pw != null) {
                pw.println("ts_ms,group,partition,end_offset,committed_offset,lag");
            }
            if (mw != null) {
                mw.println("ts_ms,group,partition,consumer_id,client_id,host");
            }

            Map<String, Map<Integer, String>> lastAssignment = new HashMap<>();
            while (!Thread.currentThread().isInterrupted()) {
                long t0 = System.currentTimeMillis();
                StringBuilder line = new StringBuilder().append(t0);
                for (Map.Entry<String, String> e : groupToTopic.entrySet()) {
                    String group = e.getKey();
                    List<TopicPartition> tps = partitions.get(e.getValue());

                    GroupSample s = sample(admin, group, tps);
                    line.append(',').append(s.total());
                    if (pw != null) {
                        for (var pe : s.perPartition().entrySet()) {
                            long end = pe.getValue()[0];
                            long committed = pe.getValue()[1];
                            long lag = committed < 0 ? -1 : end - committed;
                            pw.println(t0 + "," + group + "," + pe.getKey().partition() + ","
                                    + end + "," + committed + "," + lag);
                        }
                    }
                    if (mw != null) {
                        Map<Integer, String> now = assignment(admin, group, tps);
                        if (now != null && !now.equals(lastAssignment.get(group))) {
                            for (var me : now.entrySet()) {
                                mw.println(t0 + "," + group + "," + me.getKey() + "," + me.getValue());
                            }
                            lastAssignment.put(group, now);
                        }
                    }
                }
                w.println(line);
                long sleep = intervalMs - (System.currentTimeMillis() - t0);
                if (sleep > 0) {
                    TimeUnit.MILLISECONDS.sleep(sleep);
                }
            }
        }
    }

    private static PrintWriter optionalWriter(String path) throws IOException {
        return path.isEmpty() ? null : new PrintWriter(new FileWriter(path, false), true);
    }

    /** 파티션별 {끝 오프셋, 커밋 오프셋}. 커밋 이력이 없는 파티션은 커밋 오프셋 -1, 합계 -1. */
    private static GroupSample sample(Admin admin, String group, List<TopicPartition> tps) {
        try {
            Map<TopicPartition, OffsetSpec> req = new HashMap<>();
            tps.forEach(tp -> req.put(tp, OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin.listOffsets(req).all().get();
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();

            Map<TopicPartition, long[]> per = new TreeMap<>(
                    (x, y) -> Integer.compare(x.partition(), y.partition()));
            long sum = 0;
            boolean missing = false;
            for (TopicPartition tp : tps) {
                long end = ends.get(tp).offset();
                OffsetAndMetadata c = committed.get(tp);
                if (c == null) {
                    missing = true;
                    per.put(tp, new long[]{end, -1});
                } else {
                    sum += end - c.offset();
                    per.put(tp, new long[]{end, c.offset()});
                }
            }
            return new GroupSample(missing ? -1 : sum, per);
        } catch (Exception ex) {
            return GroupSample.FAILED;
        }
    }

    /** 파티션 번호 → "consumer_id,client_id,host". 담당이 없으면 "-,-,-". 조회 실패 시 null. */
    private static Map<Integer, String> assignment(Admin admin, String group, List<TopicPartition> tps) {
        try {
            ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(group))
                    .all().get().get(group);
            Map<Integer, String> m = new TreeMap<>();
            for (TopicPartition tp : tps) {
                m.put(tp.partition(), "-,-,-");
            }
            for (MemberDescription md : d.members()) {
                for (TopicPartition tp : md.assignment().topicPartitions()) {
                    if (m.containsKey(tp.partition()) && tps.contains(tp)) {
                        m.put(tp.partition(), md.consumerId() + "," + md.clientId() + "," + md.host());
                    }
                }
            }
            return m;
        } catch (Exception ex) {
            return null;
        }
    }
}