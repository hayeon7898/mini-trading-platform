package com.mts.orderserver.simulator;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * 체결 시뮬레이터 설정.
 *
 * @param enabled     스케줄러 실행 여부. 기본 false라 테스트에서는 돌지 않는다.
 * @param interval    워커가 한 배치를 끝낸 뒤 다음 배치까지 쉬는 시간
 * @param batchSize   한 트랜잭션에서 잠가서 체결할 최대 주문 수
 * @param workers     동시에 도는 워커(스레드) 수
 * @param priceSpread 체결가 변동 폭. 0.01이면 base_price ±1%
 */
@ConfigurationProperties("simulator")
public record SimulatorProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("100ms") Duration interval,
        @DefaultValue("50") int batchSize,
        @DefaultValue("2") int workers,
        @DefaultValue("0.01") BigDecimal priceSpread
) {
}
