package mts.experiments;

import java.util.Arrays;

/**
 * E1 실험 도구 진입점.
 * <pre>
 *   producer-tool produce --topic T --acks 1|all --idempotence true|false --rate 5000 --duration 60 --out /out
 *   producer-tool verify  --topic T --out /out
 * </pre>
 */
public final class Main {

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            usage();
            System.exit(1);
        }
        Args args = Args.parse(Arrays.copyOfRange(argv, 1, argv.length));
        switch (argv[0]) {
            case "produce" -> SeqProducer.run(args);
            case "verify" -> Verifier.run(args);
            default -> {
                usage();
                System.exit(1);
            }
        }
    }

    private static void usage() {
        System.err.println("""
            usage:
              produce --topic T [--acks all] [--idempotence false] [--rate 5000] [--duration 60] [--out /out]
                      [--bootstrap host:port,...] [--request-timeout-ms N] [--delivery-timeout-ms N] [--linger-ms N]
                      [--producer-config k=v,k=v]
              verify  --topic T [--out /out] [--bootstrap host:port,...] [--timeout-sec 120]
            """);
    }
}