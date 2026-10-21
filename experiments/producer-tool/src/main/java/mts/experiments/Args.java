package mts.experiments;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** "--key value" 형식의 단순 인자 파서. */
final class Args {

    static final String DEFAULT_BOOTSTRAP = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    private final Map<String, String> values;

    private Args(Map<String, String> values) {
        this.values = values;
    }

    static Args parse(String[] argv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String key = argv[i];
            if (!key.startsWith("--") || i + 1 >= argv.length) {
                throw new IllegalArgumentException("잘못된 인자: " + key);
            }
            map.put(key.substring(2), argv[++i]);
        }
        return new Args(map);
    }

    String get(String key, String defaultValue) {
        return values.getOrDefault(key, defaultValue);
    }

    int getInt(String key, int defaultValue) {
        String v = values.get(key);
        return v == null ? defaultValue : Integer.parseInt(v);
    }

    Optional<String> optional(String key) {
        return Optional.ofNullable(values.get(key));
    }

    String require(String key) {
        String v = values.get(key);
        if (v == null) {
            throw new IllegalArgumentException("--" + key + " 필수");
        }
        return v;
    }
}
