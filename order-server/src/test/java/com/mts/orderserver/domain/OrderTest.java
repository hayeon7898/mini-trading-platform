package com.mts.orderserver.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private static final LocalDateTime T0 = LocalDateTime.of(2026, 10, 15, 0, 0, 0);
    private static final LocalDateTime T1 = T0.plusSeconds(5);

    private Order newOrder() {
        return Order.accept(1L, "005930", OrderSide.BUY, new BigDecimal("71000.0000"), 10, T0);
    }

    /** 체결 로직(fill)은 #12에서 추가된다. 그 전까지 상태를 직접 세팅한다. */
    private Order orderIn(OrderStatus status, int filledQty) {
        Order order = newOrder();
        ReflectionTestUtils.setField(order, "status", status);
        ReflectionTestUtils.setField(order, "filledQty", filledQty);
        return order;
    }

    @Test
    void 접수하면_ACCEPTED_상태이고_체결수량은_0이다() {
        Order order = newOrder();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.ACCEPTED);
        assertThat(order.getFilledQty()).isZero();
        assertThat(order.getCreatedAt()).isEqualTo(T0);
        assertThat(order.getUpdatedAt()).isEqualTo(T0);
    }

    @Test
    void 수량이_0_이하면_접수할_수_없다() {
        assertThatThrownBy(() ->
                Order.accept(1L, "005930", OrderSide.BUY, new BigDecimal("71000"), 0, T0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 가격이_0_이하면_접수할_수_없다() {
        assertThatThrownBy(() ->
                Order.accept(1L, "005930", OrderSide.BUY, BigDecimal.ZERO, 10, T0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ACCEPTED_주문을_취소하면_CANCELED가_되고_갱신시각이_바뀐다() {
        Order order = newOrder();

        order.cancel(T1);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELED);
        assertThat(order.getUpdatedAt()).isEqualTo(T1);
        assertThat(order.getCreatedAt()).isEqualTo(T0);
    }

    @Test
    void PARTIAL_주문을_취소하면_체결된_수량은_유지된다() {
        Order order = orderIn(OrderStatus.PARTIAL, 4);

        order.cancel(T1);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELED);
        assertThat(order.getFilledQty()).isEqualTo(4);
    }

    @ParameterizedTest
    @EnumSource(value = OrderStatus.class, names = {"FILLED", "CANCELED"})
    void 잔량이_없는_주문은_취소할_수_없고_상태도_바뀌지_않는다(OrderStatus status) {
        Order order = orderIn(status, status == OrderStatus.FILLED ? 10 : 0);

        assertThatThrownBy(() -> order.cancel(T1))
                .isInstanceOf(OrderNotCancelableException.class);
        assertThat(order.getStatus()).isEqualTo(status);
        assertThat(order.getUpdatedAt()).isEqualTo(T0);
    }
}
