package com.mts.orderserver.api.dto;

import com.mts.orderserver.domain.Execution;
import com.mts.orderserver.domain.OrderSide;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;

/** 시각은 Instant로 내보내 "Z"가 붙은 ISO-8601(UTC)이 되게 한다. */
public record ExecutionResponse(
        Long id,
        Long orderId,
        Long accountId,
        String symbol,
        OrderSide side,
        BigDecimal price,
        int quantity,
        Instant executedAt
) {
    public static ExecutionResponse from(Execution e) {
        return new ExecutionResponse(
                e.getId(), e.getOrderId(), e.getAccountId(), e.getSymbol(), e.getSide(),
                e.getPrice(), e.getQuantity(), e.getExecutedAt().toInstant(ZoneOffset.UTC));
    }
}
