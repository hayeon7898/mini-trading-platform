package com.mts.orderserver.api;

import com.jayway.jsonpath.JsonPath;
import com.mts.orderserver.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 주문 내역 페이지 이동 (OFFSET, 커서).
 * 시각이 같은 주문을 일부러 섞어, 커서가 id로 이어져 누락·중복이 없는지 확인한다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@Sql("/sql/order-fixture.sql")
class OrderPaginationApiTest {

    private static final int TOTAL = 50;

    @Autowired
    WebApplicationContext context;

    @Autowired
    JdbcTemplate jdbc;

    MockMvc mvc;

    /** 최신순(created_at DESC, id DESC)으로 정렬한 전체 주문 id */
    List<Long> expectedIds;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
        LocalDateTime base = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS).minusDays(1);
        for (int i = 0; i < TOTAL; i++) {
            // 3건씩 같은 시각 → 페이지 경계에서 동률이 생긴다
            LocalDateTime at = base.minusSeconds(i / 3);
            jdbc.update("""
                    INSERT INTO orders (account_id, symbol, side, price, quantity, filled_qty, status, created_at, updated_at)
                    VALUES (1, '005930', 'BUY', 71000, ?, 0, 'ACCEPTED', ?, ?)
                    """, i + 1, at, at);
        }
        expectedIds = jdbc.queryForList(
                "SELECT id FROM orders WHERE account_id = 1 ORDER BY created_at DESC, id DESC", Long.class);
    }

    @Test
    void 커서로_끝까지_넘기면_누락과_중복_없이_전체를_최신순으로_받는다() throws Exception {
        List<Long> seen = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            var req = get("/accounts/1/orders").param("size", "7");
            if (cursor != null) {
                req.param("cursor", cursor);
            }
            String body = mvc.perform(req).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            List<Number> ids = JsonPath.read(body, "$.items[*].id");
            ids.forEach(id -> seen.add(id.longValue()));
            cursor = JsonPath.read(body, "$.nextCursor");
            pages++;
        } while (cursor != null && pages < 20);

        assertThat(seen).containsExactlyElementsOf(expectedIds);
    }

    @Test
    void 마지막_페이지에서는_nextCursor가_null이다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("size", "100"))
                .andExpect(jsonPath("$.nextCursor", nullValue()));
    }

    @Test
    void page로_OFFSET_방식_조회를_할_수_있다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("size", "2").param("page", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[*].id", contains(
                        expectedIds.get(2).intValue(), expectedIds.get(3).intValue())));
    }

    @Test
    void page와_cursor를_함께_보내면_400이다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("page", "1").param("cursor", "MTox"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void 잘못된_커서면_400이다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("cursor", "not-a-cursor"))
                .andExpect(status().isBadRequest());
    }
}
