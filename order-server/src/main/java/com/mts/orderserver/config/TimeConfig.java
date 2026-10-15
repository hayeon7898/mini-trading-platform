package com.mts.orderserver.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * 원장 시각은 UTC로 저장한다 (Debezium이 DATETIME을 UTC 기준 epoch millis로 내보내기 때문).
 * 시각은 항상 이 Clock으로 만들고, 테스트에서는 고정 Clock으로 바꿔 끼운다.
 */
@Configuration(proxyBeanMethods = false)
public class TimeConfig {

    @Bean
    public Clock clock() {
        return Clock.tick(Clock.systemUTC(), Duration.ofMillis(1));
    }
}
