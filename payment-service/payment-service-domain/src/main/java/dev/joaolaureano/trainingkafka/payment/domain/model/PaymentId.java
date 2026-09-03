package dev.joaolaureano.trainingkafka.payment.domain.model;

import java.util.UUID;

public record PaymentId(UUID value) {

    public PaymentId {
        if (value == null) {
            throw new IllegalArgumentException("paymentId is required");
        }
    }

    public static PaymentId generate() {
        return new PaymentId(UUID.randomUUID());
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
