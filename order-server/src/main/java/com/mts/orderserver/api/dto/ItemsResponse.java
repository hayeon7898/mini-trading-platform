package com.mts.orderserver.api.dto;

import java.util.List;

/**
 * 목록 응답. 배열 대신 객체로 감싸, 05 스펙에서 커서(nextCursor) 같은 필드를
 * 추가해도 기존 응답이 깨지지 않게 한다.
 */
public record ItemsResponse<T>(List<T> items) {
}
