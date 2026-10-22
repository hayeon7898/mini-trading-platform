package mts.experiments;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 일련번호(1, 2, 3, ...)를 정해진 속도로 보내고, 레코드마다 성공(ack)/실패를 파일로 남긴다.
 *
 * <p>출력 (--out 디렉토리)
 * <ul>
 *   <li>acked.csv    : seq, offset, ack_ms  — 브로커가 ack한 레코드</li>
 *   <li>failed.csv   : seq, error, fail_ms  — 최종 실패로 콜백된 레코드</li>
 *   <li>timeline.csv : 초 단위 sent / acked / failed</li>
 *   <li>summary.txt  : 설정과 집계, 가장 긴 ack 공백 구간</li>
 * </ul>
 * 유실 판정은 Verifier가 acked.csv와 토픽 내용을 비교해 한다.
 */
final class SeqProducer {

    private static final DateTimeFormatter TS =
        DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    /** 전송 구간 이후 재시도·flush로 콜백이 늦게 오는 시간까지 여유 있게 잡는다. */
    private static final int TIMELINE_SLACK_SEC = 600;

    static void run(Args a) throws Exception {
        String bootstrap = a.get("bootstrap", Args.DEFAULT_BOOTSTRAP);
        String topic = a.require("topic");
        String acks = a.get("acks", "all");
        String idempotence = a.get("idempotence", "false");
        int rate = a.getInt("rate", 5000);
        int duration = a.getInt("duration", 60);
        Path out = Path.of(a.get("out", "/out"));
        Files.createDirectories(out);
        // seq: 값 = 일련번호 문자열 (E1) / execution: mts.trading.executions와 같은 JSON (E4)
        String format = a.get("format", "seq");
        // none: 키 없음 (E1) / id: Debezium과 같은 {"id":N} 키로 파티션 분산 (E4)
        String keyMode = a.get("key", "execution".equals(format) ? "id" : "none");
        // false면 레코드별 acked.csv / failed.csv를 쓰지 않음 (대량 적재 시 파일 I/O가 병목이 되지 않게)
        boolean recordEach = Boolean.parseBoolean(a.get("record-each", "true"));
        if (!format.equals("seq") && !format.equals("execution")) {
            throw new IllegalArgumentException("--format seq|execution");
        }

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "e1-producer");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, acks);
        // 명시하지 않으면 acks=1일 때 멱등성이 조용히 꺼진다. 조합을 분명히 하기 위해 항상 명시
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
        a.optional("request-timeout-ms").ifPresent(v -> p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, v));
        a.optional("delivery-timeout-ms").ifPresent(v -> p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, v));
        a.optional("linger-ms").ifPresent(v -> p.put(ProducerConfig.LINGER_MS_CONFIG, v));
        // 그 밖의 프로듀서 설정: --producer-config k=v,k=v (실험 변수용)
        a.optional("producer-config").ifPresent(s -> {
            for (String kv : s.split(",")) {
                int i = kv.indexOf('=');
                if (i <= 0) {
                    throw new IllegalArgumentException("--producer-config 형식: k=v,k=v");
                }
                p.put(kv.substring(0, i).trim(), kv.substring(i + 1).trim());
            }
        });

        long total = (long) rate * duration;
        if (total > Integer.MAX_VALUE - 1) {
            throw new IllegalArgumentException("rate × duration이 너무 큼: " + total);
        }

        Recorder rec = new Recorder(out, (int) total, duration + TIMELINE_SLACK_SEC, recordEach);
        log("start topic=%s acks=%s idempotence=%s rate=%d/s duration=%ds total=%d format=%s key=%s",
            topic, acks, idempotence, rate, duration, total, format, keyMode);

        ScheduledExecutorService progress = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "progress");
            t.setDaemon(true);
            return t;
        });
        progress.scheduleAtFixedRate(rec::printProgress, 5, 5, TimeUnit.SECONDS);

        long startNs = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (long seq = 1; seq <= total; seq++) {
                // 고정 속도: seq번째 레코드의 목표 시각까지 대기. send가 막히면 뒤처진 만큼 몰아서 보냄
                long target = startNs + (seq - 1) * 1_000_000_000L / rate;
                long now;
                while ((now = System.nanoTime()) < target) {
                    LockSupport.parkNanos(target - now);
                }
                final int s = (int) seq;
                rec.sent();
                try {
                    String key = "id".equals(keyMode) ? "{\"id\":" + seq + "}" : null;
                    String value = "execution".equals(format)
                        ? executionJson(seq, System.currentTimeMillis())
                        : Long.toString(seq);
                    producer.send(new ProducerRecord<>(topic, key, value), (md, ex) -> {
                        if (ex == null) {
                            rec.acked(s, md.offset());
                        } else {
                            rec.failed(s, ex);
                        }
                    });
                } catch (RuntimeException e) {
                    // max.block.ms 초과 등 send 자체가 던진 예외
                    rec.failed(s, e);
                }
            }
            log("send loop done, flushing");
            producer.flush();
        } finally {
            progress.shutdownNow();
        }

        rec.finish(topic, acks, idempotence, rate, duration, total, p);
    }

    private static final String[] SYMBOLS = {
        "005930", "000660", "035420", "035720", "051910", "005380", "068270", "105560", "055550", "000270"
    };

    /** 실제 체결 CDC 메시지(ExtractNewRecordState + __op, __source_ts_ms)와 같은 필드·타입의 JSON. */
    static String executionJson(long id, long nowMs) {
        long priceX10000 = 500_000_000L + (id * 7919L) % 500_000_000L;   // 50,000.0000 ~ 99,999.9999
        long frac = priceX10000 % 10000;
        StringBuilder sb = new StringBuilder(200);
        sb.append("{\"id\":").append(id)
            .append(",\"order_id\":").append((id + 2) / 3)
            .append(",\"account_id\":").append(id % 1000 + 1)
            .append(",\"symbol\":\"").append(SYMBOLS[(int) (id % SYMBOLS.length)]).append('"')
            .append(",\"side\":\"").append((id & 1) == 0 ? "BUY" : "SELL").append('"')
            .append(",\"price\":\"").append(priceX10000 / 10000).append('.');
        if (frac < 1000) sb.append('0');
        if (frac < 100) sb.append('0');
        if (frac < 10) sb.append('0');
        sb.append(frac).append('"')
            .append(",\"quantity\":").append(id % 10 + 1)
            .append(",\"executed_at\":").append(nowMs)
            .append(",\"__op\":\"c\"")
            .append(",\"__source_ts_ms\":").append(nowMs)
            .append('}');
        return sb.toString();
    }

    static void log(String fmt, Object... args) {
        System.out.printf("[%s] %s%n", TS.format(Instant.now()), String.format(fmt, args));
    }

    /** 콜백(프로듀서 I/O 스레드)과 메인 스레드가 함께 쓰므로 기록 메서드는 동기화한다. */
    private static final class Recorder {
        private final Path out;
        private final BufferedWriter ackedOut;
        private final BufferedWriter failedOut;
        private final long[] ackMs;          // seq별 ack 시각 (0 = ack 없음)
        private final long startMs = System.currentTimeMillis();
        private final AtomicLongArray sentPerSec;
        private final AtomicLongArray ackedPerSec;
        private final AtomicLongArray failedPerSec;
        private final AtomicLong sent = new AtomicLong();
        private final AtomicLong acked = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();
        private final Map<String, LongAdder> errors = new ConcurrentHashMap<>();

        private final boolean recordEach;

        Recorder(Path out, int total, int seconds, boolean recordEach) throws IOException {
            this.out = out;
            this.recordEach = recordEach;
            this.ackedOut = Files.newBufferedWriter(out.resolve("acked.csv"));
            this.failedOut = Files.newBufferedWriter(out.resolve("failed.csv"));
            this.ackedOut.write("seq,offset,ack_ms\n");
            this.failedOut.write("seq,error,fail_ms\n");
            this.ackMs = new long[total + 1];
            this.sentPerSec = new AtomicLongArray(seconds);
            this.ackedPerSec = new AtomicLongArray(seconds);
            this.failedPerSec = new AtomicLongArray(seconds);
        }

        private int sec(long ms) {
            int s = (int) ((ms - startMs) / 1000);
            return Math.min(Math.max(s, 0), sentPerSec.length() - 1);
        }

        void sent() {
            sent.incrementAndGet();
            sentPerSec.incrementAndGet(sec(System.currentTimeMillis()));
        }

        synchronized void acked(int seq, long offset) {
            long now = System.currentTimeMillis();
            ackMs[seq] = now;
            acked.incrementAndGet();
            ackedPerSec.incrementAndGet(sec(now));
            if (recordEach) {
                write(ackedOut, seq + "," + offset + "," + now + "\n");
            }
        }

        synchronized void failed(int seq, Exception e) {
            long now = System.currentTimeMillis();
            failed.incrementAndGet();
            failedPerSec.incrementAndGet(sec(now));
            String type = e.getClass().getSimpleName();
            if (errors.putIfAbsent(type, new LongAdder()) == null) {
                log("first error %s (seq=%d): %s", type, seq, e.getMessage());
            }
            errors.get(type).increment();
            if (recordEach) {
                write(failedOut, seq + "," + type + "," + now + "\n");
            }
        }

        void printProgress() {
            log("progress sent=%d acked=%d failed=%d", sent.get(), acked.get(), failed.get());
        }

        private static void write(BufferedWriter w, String line) {
            try {
                w.write(line);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        synchronized void finish(String topic, String acks, String idempotence, int rate, int duration,
                                 long total, Properties props) throws IOException {
            ackedOut.close();
            failedOut.close();

            // ack 공백: ack 시각을 정렬해 연속 ack 사이의 가장 긴 간격 (리더 부재로 쓰기가 멈춘 구간)
            long[] times = Arrays.stream(ackMs).filter(t -> t > 0).sorted().toArray();
            long maxGap = 0;
            long gapFrom = 0;
            long gapTo = 0;
            for (int i = 1; i < times.length; i++) {
                long gap = times[i] - times[i - 1];
                if (gap > maxGap) {
                    maxGap = gap;
                    gapFrom = times[i - 1];
                    gapTo = times[i];
                }
            }

            int lastSec = 0;
            for (int i = 0; i < sentPerSec.length(); i++) {
                if (sentPerSec.get(i) + ackedPerSec.get(i) + failedPerSec.get(i) > 0) {
                    lastSec = i;
                }
            }
            try (BufferedWriter w = Files.newBufferedWriter(out.resolve("timeline.csv"))) {
                w.write("sec,epoch_ms,sent,acked,failed\n");
                for (int i = 0; i <= lastSec; i++) {
                    w.write(i + "," + (startMs + i * 1000L) + "," + sentPerSec.get(i) + ","
                        + ackedPerSec.get(i) + "," + failedPerSec.get(i) + "\n");
                }
            }

            Map<String, Object> s = new TreeMap<>();
            s.put("topic", topic);
            s.put("acks", acks);
            s.put("idempotence", idempotence);
            s.put("rate", rate);
            s.put("duration_sec", duration);
            s.put("attempted", total);
            s.put("acked", acked.get());
            s.put("failed", failed.get());
            s.put("unresolved", total - acked.get() - failed.get());
            s.put("start_ms", startMs);
            s.put("end_ms", System.currentTimeMillis());
            s.put("max_ack_gap_ms", maxGap);
            s.put("max_ack_gap_from_ms", gapFrom);
            s.put("max_ack_gap_to_ms", gapTo);
            errors.forEach((k, v) -> s.put("error." + k, v.sum()));
            props.stringPropertyNames().stream()
                .filter(k -> !k.contains("serializer"))
                .forEach(k -> s.put("config." + k, props.getProperty(k)));

            try (BufferedWriter w = Files.newBufferedWriter(out.resolve("summary.txt"))) {
                for (var e : s.entrySet()) {
                    w.write(e.getKey() + "=" + e.getValue() + "\n");
                }
            }
            log("done attempted=%d acked=%d failed=%d max_ack_gap=%dms (%s ~ %s UTC) errors=%s",
                total, acked.get(), failed.get(), maxGap,
                gapFrom == 0 ? "-" : TS.format(Instant.ofEpochMilli(gapFrom)),
                gapTo == 0 ? "-" : TS.format(Instant.ofEpochMilli(gapTo)),
                errors.isEmpty() ? "none" : errors.keySet());
        }
    }
}