package com.mts.orderserver.simulator;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SimulatorProperties.class)
public class SimulatorConfig {
}
