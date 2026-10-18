package com.mts.orderserver.api;

import com.mts.orderserver.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.matchesPattern;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@Sql("/sql/order-fixture.sql")
class OrderApiTest {

    @Autowired
    WebApplicationContext context;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    private static String body(String accountId, String symbol, String side, String price, String quantity) {
        return """
                {"accountId": %s, "symbol": %s, "side": %s, "price": %s, "quantity": %s}
                """.formatted(accountId, symbol, side, price, quantity);
    }

    private static final String VALID = body("1", "\"005930\"", "\"BUY\"", "71000", "10");

    @Test
    void 주문을_접수하면_201과_Location을_주고_조회하면_ACCEPTED다() throws Exception {
        String location = mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(VALID))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", matchesPattern("/orders/\\d+")))
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.filledQty").value(0))
                .andExpect(jsonPath("$.createdAt").value(endsWith("Z")))
                .andReturn().getResponse().getHeader("Location");

        mvc.perform(get(location))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.symbol").value("005930"))
                .andExpect(jsonPath("$.side").value("BUY"))
                .andExpect(jsonPath("$.status").value("ACCEPTED"));
    }

    @Test
    void 수량이_0이면_400이고_어느_필드인지_알려준다() throws Exception {
        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content(body("1", "\"005930\"", "\"BUY\"", "71000", "0")))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.errors[0].field").value("quantity"));
    }

    @Test
    void 가격_소수부가_4자리를_넘으면_400이다() throws Exception {
        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content(body("1", "\"005930\"", "\"BUY\"", "71000.12345", "10")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("price"));
    }

    @Test
    void side가_BUY_SELL이_아니면_400이다() throws Exception {
        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content(body("1", "\"005930\"", "\"HOLD\"", "71000", "10")))
                .andExpect(status().isBadRequest());
    }

    @Test
    void 없는_종목이면_422다() throws Exception {
        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content(body("1", "\"XXXXXX\"", "\"BUY\"", "71000", "10")))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.field").value("symbol"));
    }

    @Test
    void 없는_계좌면_422다() throws Exception {
        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content(body("999", "\"005930\"", "\"BUY\"", "71000", "10")))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.field").value("accountId"));
    }

    @Test
    void 없는_주문을_조회하면_404다() throws Exception {
        mvc.perform(get("/orders/999999"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }
}
