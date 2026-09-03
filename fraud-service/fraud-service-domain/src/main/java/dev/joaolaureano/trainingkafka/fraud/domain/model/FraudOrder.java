package dev.joaolaureano.trainingkafka.fraud.domain.model;

import java.math.BigDecimal;
import java.time.Instant;

public record FraudOrder(String orderId, Instant occurredAt, BigDecimal amount) {

    public FraudOrder {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("orderId is required");
        }
        if (occurredAt == null) {
            throw new IllegalArgumentException("occurredAt is required");
        }
        if (amount == null || amount.signum() < 0) {
            throw new IllegalArgumentException("amount must not be negative");
        }
    }
}
