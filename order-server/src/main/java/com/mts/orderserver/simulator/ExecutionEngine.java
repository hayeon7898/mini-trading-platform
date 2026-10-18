package com.mts.orderserver.simulator;

import com.mts.orderserver.domain.Execution;
import com.mts.orderserver.domain.ExecutionRepository;
import com.mts.orderserver.domain.Order;
import com.mts.orderserver.domain.OrderRepository;
import com.mts.orderserver.domain.Stock;
import com.mts.orderserver.domain.StockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 체결 한 배치를 한 트랜잭션으로 처리한다. 스케줄러(워커)와 테스트가 직접 호출한다.
 */
@Service
public class ExecutionEngine {

    private final OrderRepository orderRepository;
    private final ExecutionRepository executionRepository;
    private final StockRepository stockRepository;
    private final SimulatorProperties properties;
    private final Clock clock;

    public ExecutionEngine(OrderRepository orderRepository,
                           ExecutionRepository executionRepository,
                           StockRepository stockRepository,
                           SimulatorProperties properties,
                           Clock clock) {
        this.orderRepository = orderRepository;
        this.executionRepository = executionRepository;
        this.stockRepository = stockRepository;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * 잔량이 남은 주문을 최대 batchSize건 잠그고, 각각 일부 또는 전부를 체결한다.
     * 체결 기록 INSERT와 주문 UPDATE가 같은 트랜잭션이라 함께 커밋되거나 함께 롤백된다.
     *
     * 격리 수준 READ COMMITTED: 기본값(REPEATABLE READ)의 잠금 읽기는 행 사이 틈(gap)까지 잠가,
     * 주문 접수 API의 INSERT가 그 틈에 걸려 기다릴 수 있다. READ COMMITTED는 행만 잠근다.
     *
     * @return 체결한 주문 수
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public int executeBatch(int batchSize) {
        List<Order> orders = orderRepository.lockFillable(batchSize);
        if (orders.isEmpty()) {
            return 0;
        }

        Map<String, BigDecimal> basePrices = stockRepository
                .findAllById(orders.stream().map(Order::getSymbol).distinct().toList())
                .stream()
                .collect(Collectors.toMap(Stock::getSymbol, Stock::getBasePrice));
        LocalDateTime now = LocalDateTime.now(clock);

        List<Execution> executions = orders.stream()
                .map(order -> order.fill(
                        randomQuantity(order.remainingQty()),
                        randomPrice(basePrices.get(order.getSymbol())),
                        now))
                .toList();
        executionRepository.saveAll(executions);
        // 주문 변경은 커밋 시 dirty checking으로 UPDATE된다.
        return executions.size();
    }

    /** 1 ~ remaining 사이 균등 분포. */
    private int randomQuantity(int remaining) {
        return ThreadLocalRandom.current().nextInt(1, remaining + 1);
    }

    /** base_price × (1 ± priceSpread), 소수 4자리. */
    private BigDecimal randomPrice(BigDecimal basePrice) {
        double spread = properties.priceSpread().doubleValue();
        double factor = 1 + ThreadLocalRandom.current().nextDouble(-spread, spread);
        return basePrice.multiply(BigDecimal.valueOf(factor)).setScale(4, RoundingMode.HALF_UP);
    }
}
