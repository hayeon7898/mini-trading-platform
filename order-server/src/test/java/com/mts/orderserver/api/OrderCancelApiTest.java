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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@Sql("/sql/order-fixture.sql")
class OrderCancelApiTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    JdbcTemplate jdbc;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    /** API로 주문을 접수하고 Location 헤더에서 ID를 꺼낸다. */
    private long placeOrder() throws Exception {
        String location = mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"accountId": 1, "symbol": "005930", "side": "BUY", "price": 71000, "quantity": 10}
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getHeader("Location");
        return Long.parseLong(location.substring(location.lastIndexOf('/') + 1));
    }

    /** 체결 로직(#12) 전이라 체결 상태는 DB에 직접 만든다. */
    private void setState(long orderId, String status, int filledQty) {
        jdbc.update("UPDATE orders SET status = ?, filled_qty = ? WHERE id = ?", status, filledQty, orderId);
    }

    @Test
    void ACCEPTED_주문을_취소하면_200과_CANCELED를_주고_조회해도_CANCELED다() throws Exception {
        long id = placeOrder();

        mvc.perform(post("/orders/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELED"));

        mvc.perform(get("/orders/{id}", id))
                .andExpect(jsonPath("$.status").value("CANCELED"));
    }

    @Test
    void PARTIAL_주문을_취소하면_체결수량은_유지된다() throws Exception {
        long id = placeOrder();
        setState(id, "PARTIAL", 4);

        mvc.perform(post("/orders/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELED"))
                .andExpect(jsonPath("$.filledQty").value(4));
    }

    @Test
    void 이미_취소된_주문을_다시_취소하면_409다() throws Exception {
        long id = placeOrder();
        mvc.perform(post("/orders/{id}/cancel", id)).andExpect(status().isOk());

        mvc.perform(post("/orders/{id}/cancel", id))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.orderStatus").value("CANCELED"));
    }

    @Test
    void FILLED_주문을_취소하면_409이고_상태는_그대로다() throws Exception {
        long id = placeOrder();
        setState(id, "FILLED", 10);

        mvc.perform(post("/orders/{id}/cancel", id))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.orderStatus").value("FILLED"));

        mvc.perform(get("/orders/{id}", id))
                .andExpect(jsonPath("$.status").value("FILLED"));
    }

    @Test
    void 없는_주문을_취소하면_404다() throws Exception {
        mvc.perform(post("/orders/{id}/cancel", 999999))
                .andExpect(status().isNotFound());
    }
}
