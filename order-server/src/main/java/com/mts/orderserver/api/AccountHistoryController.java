package com.mts.orderserver.api;

import com.mts.orderserver.api.dto.ExecutionResponse;
import com.mts.orderserver.api.dto.ItemsResponse;
import com.mts.orderserver.api.dto.OrderResponse;
import com.mts.orderserver.domain.Order;
import com.mts.orderserver.domain.OrderStatus;
import com.mts.orderserver.service.AccountHistoryService;
import com.mts.orderserver.service.OrderCursor;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/**
 * 계좌별 주문·체결 내역.
 * from/to는 "2026-10-18T00:00:00Z" 형식(UTC). 생략하면 최근 30일.
 *
 * 주문 내역 페이지 이동
 * - cursor (권장): 응답의 nextCursor를 그대로 넘긴다. 깊이와 무관하게 비용이 일정하다.
 * - page: OFFSET 방식. 깊은 페이지일수록 건너뛸 행을 모두 읽어 느려진다 (비교용).
 */
@RestController
@RequestMapping("/accounts/{accountId}")
public class AccountHistoryController {

    private final AccountHistoryService service;

    public AccountHistoryController(AccountHistoryService service) {
        this.service = service;
    }

    @GetMapping("/orders")
    public ItemsResponse<OrderResponse> orders(
            @PathVariable Long accountId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) OrderStatus status,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size,
            @RequestParam(required = false) @Min(0) Integer page,
            @RequestParam(required = false) String cursor) {
        OrderCursor after = (cursor == null || cursor.isBlank()) ? null : OrderCursor.decode(cursor);
        List<Order> orders = service.orders(accountId, from, to, status, size, page, after);

        String next = (orders.size() == size) ? OrderCursor.after(orders.getLast()).encode() : null;
        return new ItemsResponse<>(orders.stream().map(OrderResponse::from).toList(), next);
    }

    @GetMapping("/executions")
    public ItemsResponse<ExecutionResponse> executions(
            @PathVariable Long accountId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return ItemsResponse.of(service.executions(accountId, from, to, size).stream()
                .map(ExecutionResponse::from)
                .toList());
    }
}