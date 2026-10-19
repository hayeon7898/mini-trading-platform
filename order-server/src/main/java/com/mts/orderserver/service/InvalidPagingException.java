package com.mts.orderserver.service;

/** 잘못된 커서, 또는 page와 cursor를 함께 보낸 경우. API에서 400으로 매핑한다. */
public class InvalidPagingException extends RuntimeException {

    public InvalidPagingException(String message) {
        super(message);
    }
}
