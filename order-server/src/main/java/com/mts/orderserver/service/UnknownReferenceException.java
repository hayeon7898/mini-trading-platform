package com.mts.orderserver.service;

/**
 * 주문이 참조하는 종목·계좌가 없을 때. API에서 422로 매핑한다.
 * 요청 형식은 맞지만(400 아님) 업무 규칙상 처리할 수 없는 요청이라는 뜻.
 * DB에 FK가 없으므로(05 스펙 기준선 유지) 존재 확인은 애플리케이션이 맡는다.
 */
public class UnknownReferenceException extends RuntimeException {

    private final String field;

    public UnknownReferenceException(String field, Object value) {
        super("%s '%s' does not exist".formatted(field, value));
        this.field = field;
    }

    public String getField() { return field; }
}
