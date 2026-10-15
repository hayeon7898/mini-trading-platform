package com.mts.orderserver.service;

/** 주문이 없을 때. API에서 404로 매핑한다. */
public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(Long orderId) {
        super("order %d not found".formatted(orderId));
    }
}
