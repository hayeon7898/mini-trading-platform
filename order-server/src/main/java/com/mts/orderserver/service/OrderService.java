package com.mts.orderserver.service;

import com.mts.orderserver.domain.AccountRepository;
import com.mts.orderserver.domain.Order;
import com.mts.orderserver.domain.OrderRepository;
import com.mts.orderserver.domain.OrderSide;
import com.mts.orderserver.domain.StockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final StockRepository stockRepository;
    private final AccountRepository accountRepository;
    private final Clock clock;

    public OrderService(OrderRepository orderRepository,
                        StockRepository stockRepository,
                        AccountRepository accountRepository,
                        Clock clock) {
        this.orderRepository = orderRepository;
        this.stockRepository = stockRepository;
        this.accountRepository = accountRepository;
        this.clock = clock;
    }

    /**
     * 주문 접수. 종목·계좌 존재를 매 요청마다 조회한다 (요청당 SELECT 2회 추가).
     * 부하 테스트(#15)에서 병목이면 종목은 메모리 캐시로 바꾸는 것을 검토한다.
     */
    @Transactional
    public Order place(Long accountId, String symbol, OrderSide side, BigDecimal price, int quantity) {
        if (!stockRepository.existsById(symbol)) {
            throw new UnknownReferenceException("symbol", symbol);
        }
        if (!accountRepository.existsById(accountId)) {
            throw new UnknownReferenceException("accountId", accountId);
        }
        Order order = Order.accept(accountId, symbol, side, price, quantity, LocalDateTime.now(clock));
        return orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    public Order get(Long orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
