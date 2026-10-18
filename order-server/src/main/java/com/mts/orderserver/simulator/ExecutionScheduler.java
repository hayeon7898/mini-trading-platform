package com.mts.orderserver.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 워커 N개가 각자 interval마다 ExecutionEngine.executeBatch()를 호출한다.
 * 워커끼리는 SKIP LOCKED로 서로 다른 주문을 가져가므로 같은 주문을 두 번 체결하지 않는다.
 *
 * simulator.enabled=true일 때만 빈이 만들어진다.
 * SmartLifecycle이라 앱 종료 시 DataSource가 닫히기 전에 먼저 멈춘다.
 */
@Component
@ConditionalOnProperty(name = "simulator.enabled", havingValue = "true")
public class ExecutionScheduler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ExecutionScheduler.class);

    private final ExecutionEngine engine;
    private final SimulatorProperties properties;
    private ScheduledExecutorService executor;
    private volatile boolean running;

    public ExecutionScheduler(ExecutionEngine engine, SimulatorProperties properties) {
        this.engine = engine;
        this.properties = properties;
    }

    @Override
    public void start() {
        AtomicInteger seq = new AtomicInteger();
        executor = Executors.newScheduledThreadPool(properties.workers(),
                r -> new Thread(r, "simulator-worker-" + seq.incrementAndGet()));
        long intervalMs = properties.interval().toMillis();
        for (int i = 0; i < properties.workers(); i++) {
            executor.scheduleWithFixedDelay(this::runOnce, 0, intervalMs, TimeUnit.MILLISECONDS);
        }
        running = true;
        log.info("execution simulator started: workers={}, interval={}, batchSize={}",
                properties.workers(), properties.interval(), properties.batchSize());
    }

    /** 예외가 밖으로 나가면 scheduleWithFixedDelay가 그 워커를 영구히 멈추므로 여기서 잡는다. */
    private void runOnce() {
        try {
            int filled = engine.executeBatch(properties.batchSize());
            if (filled > 0) {
                log.debug("filled {} orders", filled);
            }
        } catch (Exception e) {
            log.error("execution batch failed", e);
        }
    }

    @Override
    public void stop() {
        running = false;
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("execution simulator stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
