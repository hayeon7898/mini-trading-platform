package com.mts.orderserver.api;

import com.mts.orderserver.api.dto.ExecutionResponse;
import com.mts.orderserver.api.dto.ItemsResponse;
import com.mts.orderserver.api.dto.OrderResponse;
import com.mts.orderserver.domain.OrderStatus;
import com.mts.orderserver.service.AccountHistoryService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * 계좌별 주문·체결 내역.
 * from/to는 "2026-10-18T00:00:00Z" 형식(UTC). 생략하면 최근 30일.
 * size 범위를 벗어나면 Spring 내장 메서드 검증이 400(ProblemDetail)으로 응답한다.
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
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return new ItemsResponse<>(service.orders(accountId, from, to, status, size).stream()
                .map(OrderResponse::from)
                .toList());
    }

    @GetMapping("/executions")
    public ItemsResponse<ExecutionResponse> executions(
            @PathVariable Long accountId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return new ItemsResponse<>(service.executions(accountId, from, to, size).stream()
                .map(ExecutionResponse::from)
                .toList());
    }
}
