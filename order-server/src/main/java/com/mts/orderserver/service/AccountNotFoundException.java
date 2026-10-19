package com.mts.orderserver.service;

/** 계좌가 없을 때. API에서 404로 매핑한다. */
public class AccountNotFoundException extends RuntimeException {

    public AccountNotFoundException(Long accountId) {
        super("account %d not found".formatted(accountId));
    }
}
