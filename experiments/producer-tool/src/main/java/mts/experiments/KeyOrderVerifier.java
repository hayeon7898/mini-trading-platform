package mts.experiments;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 07 스펙 K1. 메시지 키 변경 경계 전후로 계좌별 파티션 분포와 파티션 안 순서를 검사한다.
 *
 * <ul>
 *   <li>multi_partition_accounts   : 두 개 이상의 파티션에 나타난 계좌 수</li>
 *   <li>partition_order_inversions : 파티션 안에서 binlog 위치가 앞 메시지보다 작은 건수</li>
 *   <li>account_order_inversions   : 계좌 안에서 binlog 위치가 앞 메시지보다 작은 건수</li>
 *   <li>*_ties                     : binlog 위치 세 값이 앞 메시지와 모두 같은 건수</li>
 *   <li>id_order_inversions        : 생성 이벤트에서 id가 binlog 순서와 반대로 놓인 건수</li>
 * </ul>
 *
 * id 역전은 앞쪽 --examples 건(기본 100)을 -id-inversions.csv에 예시로 남긴다.
 * 각 줄은 역전된 메시지와, 그보다 먼저 커밋됐지만 id가 더 큰 메시지를 나란히 보여준다.
 *
 * 경계 이전 메시지는 binlog 위치 필드가 없어 파티션 분포만 센다.
 * 스냅샷 메시지(__op=r)는 binlog 위치가 모두 같아 순서 검사에서 제외한다.
 *
 * 경계 파일은 kafka-get-offsets.sh 출력(topic:partition:offset) 그대로 쓴다.
 */
final class KeyOrderVerifier {

    static void run(Args a) throws Exception {
        String bootstrap = a.get("bootstrap", Args.DEFAULT_BOOTSTRAP);
        String topic = a.require("topic");
        Path out = Path.of(a.get("out", "/out"));
        Path boundaryFile = Path.of(a.require("boundary"));
        long beforeSample = Long.parseLong(a.get("before-sample", "500000"));
        long timeoutMs = a.getInt("timeout-sec", 600) * 1000L;
        int exampleLimit = a.getInt("examples", 100);

        Map<Integer, Long> boundary = readBoundary(boundaryFile, topic);

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   // 그룹 없이 assign으로 읽음
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

        // 계좌 → 나타난 파티션 비트마스크
        Map<Long, Integer> beforeAccounts = new HashMap<>();
        Map<Long, Integer> afterAccounts = new HashMap<>();
        // 순서 검사용 마지막 binlog 위치 (file 번호, pos, row)
        Map<Integer, long[]> lastByPartition = new HashMap<>();
        Map<Long, long[]> lastByAccount = new HashMap<>();
        // 계좌별로 지금까지 본 가장 큰 생성 id와 그 메시지의 위치
        Map<Long, Seen> maxIdByAccount = new HashMap<>();
        List<String> idExamples = new ArrayList<>();

        long beforeRecords = 0, afterRecords = 0;
        long snapshotRecords = 0, tombstones = 0, unparsed = 0, missingSource = 0;
        long orderChecked = 0;
        long partitionInversions = 0, partitionTies = 0;
        long accountInversions = 0, accountTies = 0;
        long idChecked = 0, idInversions = 0;

        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream()
                .map(pi -> new TopicPartition(topic, pi.partition()))
                .toList();
            for (TopicPartition tp : tps) {
                if (!boundary.containsKey(tp.partition())) {
                    throw new IllegalArgumentException("경계 파일에 파티션이 없음: " + tp);
                }
                if (tp.partition() >= 31) {
                    throw new IllegalArgumentException("파티션 비트마스크는 31개까지만 지원: " + tp);
                }
            }
            c.assign(tps);
            Map<TopicPartition, Long> begin = c.beginningOffsets(tps);
            for (TopicPartition tp : tps) {
                long from = Math.max(begin.get(tp), boundary.get(tp.partition()) - beforeSample);
                c.seek(tp, from);
            }
            Map<TopicPartition, Long> end = c.endOffsets(tps);

            long deadline = System.currentTimeMillis() + timeoutMs;
            while (!reachedEnd(c, tps, end)) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("끝 오프셋까지 읽지 못함 (timeout): " + end);
                }
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                    int part = r.partition();
                    boolean after = r.offset() >= boundary.get(part);
                    String value = r.value();

                    if (value == null) {          // tombstone
                        tombstones++;
                        continue;
                    }
                    Long account = r.key() == null ? null : num(r.key(), "account_id");
                    if (account == null) {
                        account = num(value, "account_id");  // 경계 이전 키는 {"id":...}
                    }
                    if (account == null) {
                        unparsed++;
                        continue;
                    }

                    if (!after) {
                        beforeRecords++;
                        beforeAccounts.merge(account, 1 << part, (x, y) -> x | y);
                        continue;
                    }
                    afterRecords++;
                    afterAccounts.merge(account, 1 << part, (x, y) -> x | y);

                    String op = str(value, "__op");
                    if ("r".equals(op)) {
                        snapshotRecords++;
                        continue;
                    }
                    String file = str(value, "__source_file");
                    Long pos = num(value, "__source_pos");
                    Long row = num(value, "__source_row");
                    if (file == null || pos == null || row == null) {
                        missingSource++;
                        continue;
                    }
                    long[] cur = {fileNo(file), pos, row};
                    orderChecked++;

                    int cmp = compareWithLast(lastByPartition, part, cur);
                    if (cmp < 0) {
                        partitionInversions++;
                    } else if (cmp == 0) {
                        partitionTies++;
                    }

                    cmp = compareWithLast(lastByAccount, account, cur);
                    if (cmp < 0) {
                        accountInversions++;
                    } else if (cmp == 0) {
                        accountTies++;
                    }

                    // 주문 토픽의 update는 예전 주문의 id라 비교 대상에서 뺀다.
                    if ("c".equals(op)) {
                        Long id = num(value, "id");
                        if (id != null) {
                            idChecked++;
                            Long tsMs = num(value, "__source_ts_ms");
                            Seen seen = new Seen(id, part, r.offset(), file, pos, row,
                                tsMs == null ? -1 : tsMs);
                            Seen max = maxIdByAccount.get(account);
                            if (max != null && id < max.id()) {
                                idInversions++;
                                if (idExamples.size() < exampleLimit) {
                                    idExamples.add(account + "," + seen.csv() + "," + max.csv());
                                }
                            } else {
                                maxIdByAccount.put(account, seen);
                            }
                        }
                    }
                }
            }
        }

        String safeTopic = topic.replace('.', '-');
        try (BufferedWriter w = Files.newBufferedWriter(
                out.resolve("key-order-" + safeTopic + "-multi.csv"))) {
            w.write("account_id,partitions\n");
            for (var e : new TreeMap<>(afterAccounts).entrySet()) {
                if (Integer.bitCount(e.getValue()) > 1) {
                    w.write(e.getKey() + "," + partitions(e.getValue()) + "\n");
                }
            }
        }

        try (BufferedWriter w = Files.newBufferedWriter(
                out.resolve("key-order-" + safeTopic + "-id-inversions.csv"))) {
            w.write("account_id," + Seen.header("") + "," + Seen.header("max_") + "\n");
            for (String line : idExamples) {
                w.write(line + "\n");
            }
        }

        Map<String, Object> s = new TreeMap<>();
        s.put("topic", topic);
        s.put("before_sample_per_partition", beforeSample);
        s.put("before_records", beforeRecords);
        s.put("before_accounts", beforeAccounts.size());
        s.put("before_multi_partition_accounts", countMulti(beforeAccounts));
        s.put("after_records", afterRecords);
        s.put("after_snapshot_records", snapshotRecords);
        s.put("after_accounts", afterAccounts.size());
        s.put("after_multi_partition_accounts", countMulti(afterAccounts));
        s.put("order_checked", orderChecked);
        s.put("partition_order_inversions", partitionInversions);
        s.put("partition_order_ties", partitionTies);
        s.put("account_order_inversions", accountInversions);
        s.put("account_order_ties", accountTies);
        s.put("id_checked", idChecked);
        s.put("id_order_inversions", idInversions);
        s.put("id_inversion_examples", idExamples.size());
        s.put("missing_source_fields", missingSource);
        s.put("tombstones", tombstones);
        s.put("unparsed", unparsed);

        try (BufferedWriter w = Files.newBufferedWriter(out.resolve("key-order-" + safeTopic + ".txt"))) {
            for (var e : s.entrySet()) {
                w.write(e.getKey() + "=" + e.getValue() + "\n");
            }
        }
        s.forEach((k, v) -> System.out.println(k + "=" + v));
    }

    /** id 역전 예시 한 쪽에 남길 메시지 정보 */
    private record Seen(long id, int partition, long offset, String file, long pos, long row, long tsMs) {
        static String header(String prefix) {
            return String.join(",", prefix + "id", prefix + "partition", prefix + "offset",
                prefix + "source_file", prefix + "source_pos", prefix + "source_row", prefix + "source_ts_ms");
        }

        String csv() {
            return id + "," + partition + "," + offset + "," + file + "," + pos + "," + row + "," + tsMs;
        }
    }

    /** 앞 위치보다 작으면 음수, 같으면 0. 역전된 값으로는 기준을 바꾸지 않는다. */
    private static <K> int compareWithLast(Map<K, long[]> last, K key, long[] cur) {
        long[] prev = last.get(key);
        int cmp = prev == null ? 1 : compare(cur, prev);
        if (cmp > 0) {
            last.put(key, cur);
        }
        return cmp;
    }

    private static int compare(long[] x, long[] y) {
        for (int i = 0; i < x.length; i++) {
            int c = Long.compare(x[i], y[i]);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /** mysql-bin.000018 → 18 */
    private static long fileNo(String file) {
        return Long.parseLong(file.substring(file.lastIndexOf('.') + 1));
    }

    private static long countMulti(Map<Long, Integer> accounts) {
        return accounts.values().stream().filter(m -> Integer.bitCount(m) > 1).count();
    }

    private static String partitions(int mask) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 31; i++) {
            if ((mask & (1 << i)) != 0) {
                if (b.length() > 0) {
                    b.append(' ');
                }
                b.append(i);
            }
        }
        return b.toString();
    }

    /**
     * unwrap된 Debezium 값은 한 단계짜리 JSON이라 필드 하나씩만 꺼낸다.
     * "id"는 앞에 따옴표까지 붙여 찾으므로 "order_id"와 섞이지 않는다.
     */
    private static Long num(String json, String field) {
        String k = "\"" + field + "\":";
        int i = json.indexOf(k);
        if (i < 0) {
            return null;
        }
        int s = i + k.length();
        int e = s;
        while (e < json.length() && (Character.isDigit(json.charAt(e)) || json.charAt(e) == '-')) {
            e++;
        }
        return e == s ? null : Long.parseLong(json.substring(s, e));
    }

    private static String str(String json, String field) {
        String k = "\"" + field + "\":\"";
        int i = json.indexOf(k);
        if (i < 0) {
            return null;
        }
        int s = i + k.length();
        int e = json.indexOf('"', s);
        return e < 0 ? null : json.substring(s, e);
    }

    private static Map<Integer, Long> readBoundary(Path file, String topic) throws IOException {
        Map<Integer, Long> m = new HashMap<>();
        for (String line : Files.readAllLines(file)) {
            String[] f = line.trim().split(":");
            if (f.length == 3 && f[0].equals(topic)) {
                m.put(Integer.parseInt(f[1]), Long.parseLong(f[2]));
            }
        }
        if (m.isEmpty()) {
            throw new IllegalArgumentException("경계 파일에 토픽이 없음: " + topic);
        }
        return m;
    }

    private static boolean reachedEnd(KafkaConsumer<String, String> c, List<TopicPartition> tps,
                                      Map<TopicPartition, Long> end) {
        for (TopicPartition tp : tps) {
            if (c.position(tp) < end.get(tp)) {
                return false;
            }
        }
        return true;
    }
}