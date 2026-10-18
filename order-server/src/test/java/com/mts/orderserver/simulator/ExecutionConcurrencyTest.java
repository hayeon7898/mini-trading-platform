package com.mts.orderserver.simulator;

import com.mts.orderserver.TestcontainersConfiguration;
import com.mts.orderserver.domain.OrderNotCancelableException;
import com.mts.orderserver.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 체결 시뮬레이터의 동시성 검증.
 * 스케줄러 없이 ExecutionEngine을 여러 스레드에서 직접 호출해 경합을 일부러 만든다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@Sql("/sql/order-fixture.sql")
class ExecutionConcurrencyTest {

    private static final int ORDER_COUNT = 300;
    private static final int ORDER_QTY = 10;
    private static final int BATCH_SIZE = 20;

    @Autowired
    ExecutionEngine engine;

    @Autowired
    OrderService orderService;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void 워커_여러_개가_동시에_체결해도_중복_체결이_없다() throws Exception {
        insertAcceptedOrders(ORDER_COUNT);

        runConcurrently(8, i -> fillUntilNoOpenOrders());

        assertInvariants();
        assertThat(count("SELECT COUNT(*) FROM orders WHERE status <> 'FILLED'")).isZero();
        // 수량을 1~잔량에서 무작위로 정하므로 주문당 여러 번 체결되어야 정상
        assertThat(count("SELECT COUNT(*) FROM executions")).isGreaterThan(ORDER_COUNT);
    }

    @Test
    void 취소와_체결이_동시에_일어나도_취소_후_체결이나_초과_체결이_없다() throws Exception {
        List<Long> ids = insertAcceptedOrders(ORDER_COUNT);

        runConcurrently(8, i -> {
            if (i < 4) {
                fillUntilNoOpenOrders();          // 체결 워커 4개
            } else {
                cancelEvery(ids, i - 4, 4);       // 취소 스레드 4개가 주문을 나눠 취소
            }
        });

        assertInvariants();
        // 모든 주문이 닫힌 상태로 끝나야 한다
        assertThat(count("SELECT COUNT(*) FROM orders WHERE status NOT IN ('FILLED', 'CANCELED')")).isZero();
        // 취소 시각 이후에 만들어진 체결이 없어야 한다
        assertThat(count("""
                SELECT COUNT(*) FROM executions e JOIN orders o ON o.id = e.order_id
                WHERE o.status = 'CANCELED' AND e.executed_at > o.updated_at
                """)).isZero();
        // 전량 체결된 주문이 취소 상태일 수 없다
        assertThat(count("SELECT COUNT(*) FROM orders WHERE status = 'CANCELED' AND filled_qty = quantity")).isZero();

        System.out.printf("[경합 결과] FILLED=%d, CANCELED(체결 0)=%d, CANCELED(부분 체결 후)=%d%n",
                count("SELECT COUNT(*) FROM orders WHERE status = 'FILLED'"),
                count("SELECT COUNT(*) FROM orders WHERE status = 'CANCELED' AND filled_qty = 0"),
                count("SELECT COUNT(*) FROM orders WHERE status = 'CANCELED' AND filled_qty > 0"));
    }

    // ---------------------------------------------------------------- 공통 검증

    /** 이슈 #12의 불변식. */
    private void assertInvariants() {
        List<Long> mismatched = jdbc.queryForList("""
                SELECT o.id FROM orders o LEFT JOIN executions e ON e.order_id = o.id
                GROUP BY o.id, o.filled_qty
                HAVING COALESCE(SUM(e.quantity), 0) <> o.filled_qty
                """, Long.class);
        assertThat(mismatched).as("SUM(executions.quantity) <> filled_qty 인 주문").isEmpty();

        assertThat(count("SELECT COUNT(*) FROM orders WHERE filled_qty > quantity")).isZero();

        // 상태와 체결 수량이 서로 맞아야 한다
        assertThat(count("""
                SELECT COUNT(*) FROM orders WHERE
                   (status = 'ACCEPTED' AND filled_qty <> 0)
                OR (status = 'PARTIAL'  AND (filled_qty = 0 OR filled_qty >= quantity))
                OR (status = 'FILLED'   AND filled_qty <> quantity)
                """)).isZero();
    }

    // ---------------------------------------------------------------- 작업

    /** 열린 주문이 남아 있는 동안 배치 체결을 반복한다. */
    private void fillUntilNoOpenOrders() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (count("SELECT COUNT(*) FROM orders WHERE status IN ('ACCEPTED', 'PARTIAL')") > 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("체결이 60초 안에 끝나지 않음");
            }
            if (engine.executeBatch(BATCH_SIZE) == 0) {
                Thread.sleep(5); // 남은 주문을 다른 워커가 잠그고 있음
            }
        }
    }

    /** ids 중 offset부터 step 간격의 주문을 취소한다. 이미 닫힌 주문이면 건너뛴다. */
    private void cancelEvery(List<Long> ids, int offset, int step) {
        for (int i = offset; i < ids.size(); i += step) {
            try {
                orderService.cancel(ids.get(i));
            } catch (OrderNotCancelableException alreadyClosed) {
                // 체결 워커가 먼저 FILLED로 만든 경우. 정상
            }
        }
    }

    // ---------------------------------------------------------------- 도구

    @FunctionalInterface
    private interface Task {
        void run(int index) throws Exception;
    }

    /** threads개 작업을 동시에 출발시키고, 하나라도 예외가 나면 테스트를 실패시킨다. */
    private void runConcurrently(int threads, Task task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<Void>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                int index = i;
                Callable<Void> c = () -> {
                    startGate.await();
                    task.run(index);
                    return null;
                };
                futures.add(pool.submit(c));
            }
            startGate.countDown();
            for (Future<Void> f : futures) {
                f.get(90, TimeUnit.SECONDS); // 작업 중 예외는 여기서 ExecutionException으로 드러난다
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private List<Long> insertAcceptedOrders(int n) {
        jdbc.batchUpdate("""
                INSERT INTO orders (account_id, symbol, side, price, quantity, filled_qty, status, created_at, updated_at)
                VALUES (1, '005930', 'BUY', 71000, ?, 0, 'ACCEPTED', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))
                """, java.util.Collections.nCopies(n, new Object[]{ORDER_QTY}));
        return jdbc.queryForList("SELECT id FROM orders ORDER BY id", Long.class);
    }

    private long count(String sql) {
        Long n = jdbc.queryForObject(sql, Long.class);
        return n == null ? 0 : n;
    }
}
