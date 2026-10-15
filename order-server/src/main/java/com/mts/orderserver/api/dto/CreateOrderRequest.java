package com.mts.orderserver.api.dto;

import com.mts.orderserver.domain.OrderSide;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

/**
 * 제약은 DDL에 맞춘다. price는 DECIMAL(18,4)라 정수부 14자리, 소수부 4자리까지.
 * side에 BUY/SELL 외 값이 오면 역직렬화 단계에서 400이 된다.
 */
public record CreateOrderRequest(
        @NotNull @Positive Long accountId,
        @NotBlank @Size(max = 12) String symbol,
        @NotNull OrderSide side,
        @NotNull @Positive @Digits(integer = 14, fraction = 4) BigDecimal price,
        @NotNull @Positive Integer quantity
) {
}
