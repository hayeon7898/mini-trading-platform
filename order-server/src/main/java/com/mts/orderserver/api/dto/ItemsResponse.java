package com.mts.orderserver.api.dto;

import java.util.List;

/**
 * 목록 응답.
 * nextCursor: 다음 페이지를 이어 받을 커서. 마지막 페이지이거나 커서를 지원하지 않는 목록이면 null.
 */
public record ItemsResponse<T>(List<T> items, String nextCursor) {

    public static <T> ItemsResponse<T> of(List<T> items) {
        return new ItemsResponse<>(items, null);
    }
}