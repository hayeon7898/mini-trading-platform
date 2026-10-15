package com.mts.orderserver.domain;

/** 미체결 잔량이 없는 주문(FILLED, CANCELED)을 취소하려 할 때. API에서는 409로 매핑한다. */
public class OrderNotCancelableException extends RuntimeException {

    private final Long orderId;
    private final OrderStatus status;

    public OrderNotCancelableException(Long orderId, OrderStatus status) {
        super("order %d cannot be canceled in status %s".formatted(orderId, status));
        this.orderId = orderId;
        this.status = status;
    }

    public Long getOrderId() { return orderId; }
    public OrderStatus getStatus() { return status; }
}
