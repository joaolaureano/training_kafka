package dev.joaolaureano.trainingkafka.fraud.domain.model;

import java.time.Duration;

public record FraudPolicy(int maxOrders, Duration window, Duration gracePeriod) {

    public FraudPolicy {
        if (maxOrders < 1) {
            throw new IllegalArgumentException("maxOrders must be positive");
        }
        if (window == null || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be positive");
        }
        if (gracePeriod == null || gracePeriod.isNegative()) {
            throw new IllegalArgumentException("gracePeriod must not be negative");
        }
    }
}
