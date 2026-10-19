package com.mts.orderserver.service;

import com.mts.orderserver.domain.Order;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;

/**
 * 주문 내역 커서. 정렬 기준(created_at DESC, id DESC)의 마지막 행 위치를 담는다.
 * 클라이언트에는 "epochMillis:id"를 base64url로 감싼 불투명한 문자열로 준다.
 * created_at이 같은 행이 여럿이어도 id로 순서가 고정되므로 누락·중복 없이 이어진다.
 */
public record OrderCursor(LocalDateTime createdAt, long id) {

    public static OrderCursor after(Order last) {
        return new OrderCursor(last.getCreatedAt(), last.getId());
    }

    public String encode() {
        long millis = createdAt.toInstant(ZoneOffset.UTC).toEpochMilli();
        String raw = millis + ":" + id;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static OrderCursor decode(String value) {
        try {
            String raw = new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
            String[] parts = raw.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("format");
            }
            LocalDateTime at = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(parts[0])), ZoneOffset.UTC);
            return new OrderCursor(at, Long.parseLong(parts[1]));
        } catch (IllegalArgumentException e) { // NumberFormatException 포함
            throw new InvalidPagingException("invalid cursor");
        }
    }
}
