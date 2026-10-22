package mts.experiments;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
 * 토픽을 처음부터 끝(high watermark)까지 읽어 일련번호별 등장 횟수를 세고,
 * 프로듀서 기록(acked.csv, failed.csv)과 대조한다.
 *
 * <ul>
 *   <li>lost           : ack를 받았는데 토픽에 없음 → 진짜 유실</li>
 *   <li>duplicates     : 같은 일련번호가 2번 이상 (초과분 합)</li>
 *   <li>failed_present : 실패 콜백을 받았는데 토픽에 있음 → 앱이 재전송하면 중복</li>
 *   <li>out_of_order   : 오프셋 순서상 앞 번호보다 작은 번호 (재시도로 인한 순서 뒤바뀜)</li>
 * </ul>
 */
final class Verifier {

    static void run(Args a) throws Exception {
        String bootstrap = a.get("bootstrap", Args.DEFAULT_BOOTSTRAP);
        String topic = a.require("topic");
        Path out = Path.of(a.get("out", "/out"));
        long timeoutMs = a.getInt("timeout-sec", 120) * 1000L;

        Map<String, String> producerSummary = readSummary(out.resolve("summary.txt"));
        int attempted = Integer.parseInt(producerSummary.get("attempted"));
        int[] counts = new int[attempted + 1];

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   // 그룹 없이 assign으로 읽음
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

        long records = 0;
        long unknown = 0;
        long outOfOrder = 0;
        long endTotal = 0;
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream()
                .map(pi -> new TopicPartition(topic, pi.partition()))
                .toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            endTotal = end.values().stream().mapToLong(Long::longValue).sum();

            long deadline = System.currentTimeMillis() + timeoutMs;
            int maxSeen = 0;
            while (!reachedEnd(c, tps, end)) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("끝 오프셋까지 읽지 못함 (timeout): " + end);
                }
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                    records++;
                    int seq;
                    try {
                        seq = Integer.parseInt(r.value());
                    } catch (NumberFormatException | NullPointerException e) {
                        unknown++;
                        continue;
                    }
                    if (seq < 1 || seq > attempted) {
                        unknown++;
                        continue;
                    }
                    counts[seq]++;
                    if (seq < maxSeen) {
                        outOfOrder++;
                    } else {
                        maxSeen = seq;
                    }
                }
            }
        }

        long distinct = 0;
        long duplicates = 0;
        long duplicatedSeqs = 0;
        for (int seq = 1; seq <= attempted; seq++) {
            if (counts[seq] > 0) {
                distinct++;
            }
            if (counts[seq] > 1) {
                duplicatedSeqs++;
                duplicates += counts[seq] - 1;
            }
        }

        // ack 받았는데 없는 것 = 유실. 유실 레코드가 ack 때 받은 오프셋도 함께 기록
        long acked = 0;
        long lost = 0;
        long lostMinOffset = Long.MAX_VALUE;
        long lostMaxOffset = -1;
        List<String> ackedLines = Files.readAllLines(out.resolve("acked.csv"));
        try (BufferedWriter w = Files.newBufferedWriter(out.resolve("lost.csv"))) {
            w.write("seq,acked_offset\n");
            for (String line : ackedLines.subList(1, ackedLines.size())) {
                String[] f = line.split(",");
                int seq = Integer.parseInt(f[0]);
                long offset = Long.parseLong(f[1]);
                acked++;
                if (counts[seq] == 0) {
                    lost++;
                    lostMinOffset = Math.min(lostMinOffset, offset);
                    lostMaxOffset = Math.max(lostMaxOffset, offset);
                    w.write(seq + "," + offset + "\n");
                }
            }
        }

        long failed = 0;
        long failedPresent = 0;
        List<String> failedLines = Files.readAllLines(out.resolve("failed.csv"));
        for (String line : failedLines.subList(1, failedLines.size())) {
            int seq = Integer.parseInt(line.split(",")[0]);
            failed++;
            if (counts[seq] > 0) {
                failedPresent++;
            }
        }

        Map<String, Object> s = new TreeMap<>();
        s.put("topic", topic);
        s.put("attempted", attempted);
        s.put("acked", acked);
        s.put("failed", failed);
        s.put("topic_records", records);
        s.put("topic_end_offset_sum", endTotal);
        s.put("distinct_seq", distinct);
        s.put("lost", lost);
        s.put("lost_offset_range", lost == 0 ? "-" : lostMinOffset + "~" + lostMaxOffset);
        s.put("duplicates", duplicates);
        s.put("duplicated_seqs", duplicatedSeqs);
        s.put("failed_present", failedPresent);
        s.put("out_of_order", outOfOrder);
        s.put("unknown_values", unknown);

        try (BufferedWriter w = Files.newBufferedWriter(out.resolve("verify-summary.txt"))) {
            for (var e : s.entrySet()) {
                w.write(e.getKey() + "=" + e.getValue() + "\n");
            }
        }
        s.forEach((k, v) -> System.out.println(k + "=" + v));
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

    private static Map<String, String> readSummary(Path file) throws IOException {
        Map<String, String> m = new TreeMap<>();
        for (String line : Files.readAllLines(file)) {
            int i = line.indexOf('=');
            if (i > 0) {
                m.put(line.substring(0, i), line.substring(i + 1));
            }
        }
        return m;
    }
}
