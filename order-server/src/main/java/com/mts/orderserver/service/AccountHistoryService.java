package com.mts.orderserver.service;

import com.mts.orderserver.domain.AccountRepository;
import com.mts.orderserver.domain.Execution;
import com.mts.orderserver.domain.ExecutionRepository;
import com.mts.orderserver.domain.Order;
import com.mts.orderserver.domain.OrderRepository;
import com.mts.orderserver.domain.OrderStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * 계좌별 주문·체결 내역 조회.
 * 기간 기본값은 최근 30일이고 최대 기간 제한은 두지 않는다.
 */
@Service
@Transactional(readOnly = true)
public class AccountHistoryService {

    static final Duration DEFAULT_PERIOD = Duration.ofDays(30);

    private final AccountRepository accountRepository;
    private final OrderRepository orderRepository;
    private final ExecutionRepository executionRepository;
    private final Clock clock;

    public AccountHistoryService(AccountRepository accountRepository,
                                 OrderRepository orderRepository,
                                 ExecutionRepository executionRepository,
                                 Clock clock) {
        this.accountRepository = accountRepository;
        this.orderRepository = orderRepository;
        this.executionRepository = executionRepository;
        this.clock = clock;
    }

    /**
     * @param status null이면 전체 상태
     * @param page   OFFSET 방식 페이지 번호 (0부터). cursor와 함께 쓸 수 없다
     * @param cursor 커서 방식의 시작 위치. null이면 첫 페이지
     */
    public List<Order> orders(Long accountId, Instant from, Instant to, OrderStatus status,
                              int size, Integer page, OrderCursor cursor) {
        if (page != null && cursor != null) {
            throw new InvalidPagingException("page and cursor cannot be used together");
        }
        Period p = resolve(accountId, from, to);

        if (cursor != null) {
            PageRequest first = PageRequest.ofSize(size);
            return status == null
                    ? orderRepository.findAccountOrdersAfter(
                            accountId, p.from(), p.to(), cursor.createdAt(), cursor.id(), first)
                    : orderRepository.findAccountOrdersByStatusAfter(
                            accountId, status, p.from(), p.to(), cursor.createdAt(), cursor.id(), first);
        }

        PageRequest offset = PageRequest.of(page == null ? 0 : page, size);
        return status == null
                ? orderRepository.findAccountOrders(accountId, p.from(), p.to(), offset)
                : orderRepository.findAccountOrdersByStatus(accountId, status, p.from(), p.to(), offset);
    }

    public List<Execution> executions(Long accountId, Instant from, Instant to, int size) {
        Period p = resolve(accountId, from, to);
        return executionRepository.findAccountExecutions(accountId, p.from(), p.to(), PageRequest.ofSize(size));
    }

    /** 계좌 존재 확인과 기간 기본값·검증. DB 컬럼은 UTC LocalDateTime이라 변환해서 넘긴다. */
    private Period resolve(Long accountId, Instant from, Instant to) {
        if (!accountRepository.existsById(accountId)) {
            throw new AccountNotFoundException(accountId);
        }
        Instant end = (to != null) ? to : clock.instant();
        Instant start = (from != null) ? from : end.minus(DEFAULT_PERIOD);
        if (start.isAfter(end)) {
            throw new InvalidPeriodException(start, end);
        }
        return new Period(LocalDateTime.ofInstant(start, ZoneOffset.UTC),
                          LocalDateTime.ofInstant(end, ZoneOffset.UTC));
    }

    private record Period(LocalDateTime from, LocalDateTime to) {
    }
}