package com.mts.orderserver.api;

import com.mts.orderserver.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 계좌별 주문·체결 내역 API.
 * 주문마다 수량을 다르게 넣고(1, 2, 3 ...), 응답의 quantity 순서로 정렬·필터를 확인한다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@Sql("/sql/order-fixture.sql")
class AccountHistoryApiTest {

    private static final LocalDateTime NOW = LocalDateTime.now(ZoneOffset.UTC);

    @Autowired
    WebApplicationContext context;

    @Autowired
    JdbcTemplate jdbc;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
        jdbc.update("INSERT INTO accounts (id, owner_name, created_at) VALUES (2, 'other', UTC_TIMESTAMP(3))");

        // 계좌 1: 수량으로 구분 (1=3일 전, 2=2일 전 CANCELED, 3=1일 전, 4=40일 전)
        order(1, 1, "ACCEPTED", NOW.minusDays(3));
        order(1, 2, "CANCELED", NOW.minusDays(2));
        order(1, 3, "FILLED", NOW.minusDays(1));
        order(1, 4, "FILLED", NOW.minusDays(40));
        // 계좌 2: 섞이면 안 됨
        order(2, 9, "ACCEPTED", NOW.minusDays(1));

        execution(1, 5, NOW.minusDays(2));
        execution(1, 6, NOW.minusDays(1));
        execution(2, 9, NOW.minusDays(1));
    }

    private void order(long accountId, int qty, String status, LocalDateTime createdAt) {
        int filled = "FILLED".equals(status) ? qty : 0;
        jdbc.update("""
                INSERT INTO orders (account_id, symbol, side, price, quantity, filled_qty, status, created_at, updated_at)
                VALUES (?, '005930', 'BUY', 71000, ?, ?, ?, ?, ?)
                """, accountId, qty, filled, status, createdAt, createdAt);
    }

    private void execution(long accountId, int qty, LocalDateTime executedAt) {
        jdbc.update("""
                INSERT INTO executions (order_id, account_id, symbol, side, price, quantity, executed_at)
                VALUES (1, ?, '005930', 'BUY', 71000, ?, ?)
                """, accountId, qty, executedAt);
    }

    // ---------------------------------------------------------------- 주문 내역

    @Test
    void 기본_조회는_최근_30일_주문을_최신순으로_주고_다른_계좌는_섞이지_않는다() throws Exception {
        mvc.perform(get("/accounts/1/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[*].quantity", contains(3, 2, 1)));
    }

    @Test
    void 상태로_필터링한다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("status", "CANCELED"))
                .andExpect(jsonPath("$.items[*].quantity", contains(2)));
    }

    @Test
    void size만큼만_최신순으로_준다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("size", "2"))
                .andExpect(jsonPath("$.items", hasSize(2)))
                .andExpect(jsonPath("$.items[*].quantity", contains(3, 2)));
    }

    @Test
    void 기간을_지정하면_그_안의_주문만_준다() throws Exception {
        mvc.perform(get("/accounts/1/orders")
                        .param("from", NOW.minusDays(50).toInstant(ZoneOffset.UTC).toString())
                        .param("to", NOW.minusDays(30).toInstant(ZoneOffset.UTC).toString()))
                .andExpect(jsonPath("$.items[*].quantity", contains(4)));
    }

    // ---------------------------------------------------------------- 체결 내역

    @Test
    void 체결_내역을_최신순으로_주고_다른_계좌는_섞이지_않는다() throws Exception {
        mvc.perform(get("/accounts/1/executions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[*].quantity", contains(6, 5)));
    }

    // ---------------------------------------------------------------- 오류

    @Test
    void from이_to보다_늦으면_400이다() throws Exception {
        mvc.perform(get("/accounts/1/orders")
                        .param("from", "2026-10-10T00:00:00Z")
                        .param("to", "2026-10-01T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void size가_범위를_벗어나면_400이다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("size", "0"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/accounts/1/orders").param("size", "101"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void 없는_상태값이면_400이다() throws Exception {
        mvc.perform(get("/accounts/1/orders").param("status", "WRONG"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void 없는_계좌면_404다() throws Exception {
        mvc.perform(get("/accounts/999/orders"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/accounts/999/executions"))
                .andExpect(status().isNotFound());
    }
}
