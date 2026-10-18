package com.mts.orderserver.api.dto;

import com.mts.orderserver.domain.Order;
import com.mts.orderserver.domain.OrderSide;
import com.mts.orderserver.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;

/**
 * 시각은 Instant로 내보내 "Z"가 붙은 ISO-8601(UTC)이 되게 한다.
 * LocalDateTime 그대로 내보내면 시간대 표시가 없어 KST로 오해하기 쉽다.
 */
public record OrderResponse(
        Long id,
        Long accountId,
        String symbol,
        OrderSide side,
        BigDecimal price,
        int quantity,
        int filledQty,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt
) {
    public static OrderResponse from(Order o) {
        return new OrderResponse(
                o.getId(), o.getAccountId(), o.getSymbol(), o.getSide(),
                o.getPrice(), o.getQuantity(), o.getFilledQty(), o.getStatus(),
                o.getCreatedAt().toInstant(ZoneOffset.UTC),
                o.getUpdatedAt().toInstant(ZoneOffset.UTC));
    }
}
