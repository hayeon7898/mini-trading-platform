package com.mts.orderserver.api;

import com.jayway.jsonpath.JsonPath;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * DB에 저장된 시각이 실제 UTC인지 검증한다.
 *
 * Debezium은 Hibernate를 거치지 않고 DB 값을 그대로 읽어 UTC epoch millis로 내보낸다.
 * 저장·조회 변환이 서로 상쇄되면 API 응답은 정상이어도 DB(=CDC 이벤트) 값은 어긋날 수 있으므로,
 * API가 아니라 DB 원본 값을 직접 비교한다.
 *
 * 테스트 JVM은 build.gradle에서 시작부터 Asia/Seoul로 실행된다 (CI 서버가 UTC여도 동일).
 * 테스트 도중에 시간대를 바꾸면, 앞선 테스트가 만든 DB 연결이 시작 시점 시간대를 기억하고 있어
 * 운영에 없는 불일치가 생기므로 그렇게 하지 않는다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@Sql("/sql/order-fixture.sql")
class OrderTimeStorageTest {

    private static final DateTimeFormatter DB_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @Autowired
    WebApplicationContext context;

    @Autowired
    JdbcTemplate jdbc;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void JVM이_KST여도_DB에는_UTC_시각이_저장된다() throws Exception {
        String body = mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"accountId": 1, "symbol": "005930", "side": "BUY", "price": 71000, "quantity": 10}
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        Number id = JsonPath.read(body, "$.id");
        Instant apiCreatedAt = Instant.parse(JsonPath.read(body, "$.createdAt"));

        String raw = jdbc.queryForObject(
                "SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f') FROM orders WHERE id = ?",
                String.class, id.longValue());
        LocalDateTime stored = LocalDateTime.parse(raw, DB_FORMAT);

        // API 응답과 DB 원본이 같은 UTC 시각이어야 한다 (9시간 차이가 나면 실패)
        assertThat(stored).isEqualTo(LocalDateTime.ofInstant(apiCreatedAt, ZoneOffset.UTC));
        // DB 값이 실제 현재 UTC에 가까워야 한다
        assertThat(stored).isBetween(
                LocalDateTime.now(ZoneOffset.UTC).minusMinutes(1),
                LocalDateTime.now(ZoneOffset.UTC).plusMinutes(1));
    }
}