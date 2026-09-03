package dev.joaolaureano.trainingkafka.fraud.bootstrap.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("fraud")
public record FraudProperties(int maxOrders, Duration window, Duration gracePeriod) {
}
