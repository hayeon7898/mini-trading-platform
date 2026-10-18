package com.mts.orderserver.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private static final LocalDateTime T0 = LocalDateTime.of(2026, 10, 15, 0, 0, 0);
    private static final LocalDateTime T1 = T0.plusSeconds(5);
    private static final LocalDateTime T2 = T0.plusSeconds(10);
    private static final BigDecimal PRICE = new BigDecimal("71000.0000");
    private static final BigDecimal FILL_PRICE = new BigDecimal("71100.0000");

    private Order newOrder() {
        return Order.accept(1L, "005930", OrderSide.BUY, PRICE, 10, T0);
    }

    // ---------------------------------------------------------------- 접수

    @Test
    void 접수하면_ACCEPTED_상태이고_체결수량은_0이다() {
        Order order = newOrder();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.ACCEPTED);
        assertThat(order.getFilledQty()).isZero();
        assertThat(order.remainingQty()).isEqualTo(10);
        assertThat(order.getCreatedAt()).isEqualTo(T0);
        assertThat(order.getUpdatedAt()).isEqualTo(T0);
    }

    @Test
    void 수량이_0_이하면_접수할_수_없다() {
        assertThatThrownBy(() -> Order.accept(1L, "005930", OrderSide.BUY, PRICE, 0, T0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 가격이_0_이하면_접수할_수_없다() {
        assertThatThrownBy(() -> Order.accept(1L, "005930", OrderSide.BUY, BigDecimal.ZERO, 10, T0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------- 체결

    @Test
    void 일부를_체결하면_PARTIAL이고_체결기록이_만들어진다() {
        Order order = newOrder();

        Execution execution = order.fill(4, FILL_PRICE, T1);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIAL);
        assertThat(order.getFilledQty()).isEqualTo(4);
        assertThat(order.remainingQty()).isEqualTo(6);
        assertThat(order.getUpdatedAt()).isEqualTo(T1);

        assertThat(execution.getQuantity()).isEqualTo(4);
        assertThat(execution.getPrice()).isEqualTo(FILL_PRICE);
        assertThat(execution.getAccountId()).isEqualTo(1L);
        assertThat(execution.getSymbol()).isEqualTo("005930");
        assertThat(execution.getSide()).isEqualTo(OrderSide.BUY);
        assertThat(execution.getExecutedAt()).isEqualTo(T1);
    }

    @Test
    void 잔량을_모두_체결하면_FILLED다() {
        Order order = newOrder();

        order.fill(4, FILL_PRICE, T1);
        order.fill(6, FILL_PRICE, T2);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getFilledQty()).isEqualTo(10);
        assertThat(order.remainingQty()).isZero();
        assertThat(order.isOpen()).isFalse();
    }

    @Test
    void 잔량을_넘게_체결할_수_없고_상태도_바뀌지_않는다() {
        Order order = newOrder();
        order.fill(4, FILL_PRICE, T1);

        assertThatThrownBy(() -> order.fill(7, FILL_PRICE, T2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(order.getFilledQty()).isEqualTo(4);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIAL);
    }

    @Test
    void 체결수량이_0_이하면_체결할_수_없다() {
        assertThatThrownBy(() -> newOrder().fill(0, FILL_PRICE, T1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void FILLED_주문은_더_체결할_수_없다() {
        Order order = newOrder();
        order.fill(10, FILL_PRICE, T1);

        assertThatThrownBy(() -> order.fill(1, FILL_PRICE, T2))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void CANCELED_주문은_체결할_수_없다() {
        Order order = newOrder();
        order.cancel(T1);

        assertThatThrownBy(() -> order.fill(1, FILL_PRICE, T2))
                .isInstanceOf(IllegalStateException.class);
        assertThat(order.getFilledQty()).isZero();
    }

    // ---------------------------------------------------------------- 취소

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
        Order order = newOrder();
        order.fill(4, FILL_PRICE, T1);

        order.cancel(T2);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELED);
        assertThat(order.getFilledQty()).isEqualTo(4);
    }

    @Test
    void FILLED_주문은_취소할_수_없고_상태도_바뀌지_않는다() {
        Order order = newOrder();
        order.fill(10, FILL_PRICE, T1);

        assertThatThrownBy(() -> order.cancel(T2))
                .isInstanceOf(OrderNotCancelableException.class);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getUpdatedAt()).isEqualTo(T1);
    }

    @Test
    void CANCELED_주문은_다시_취소할_수_없다() {
        Order order = newOrder();
        order.cancel(T1);

        assertThatThrownBy(() -> order.cancel(T2))
                .isInstanceOf(OrderNotCancelableException.class);
        assertThat(order.getUpdatedAt()).isEqualTo(T1);
    }
}