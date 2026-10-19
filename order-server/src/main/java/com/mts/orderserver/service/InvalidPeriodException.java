package com.mts.orderserver.service;

import java.time.Instant;

/** 조회 기간의 from이 to보다 늦을 때. API에서 400으로 매핑한다. */
public class InvalidPeriodException extends RuntimeException {

    public InvalidPeriodException(Instant from, Instant to) {
        super("from (%s) must not be after to (%s)".formatted(from, to));
    }
}
