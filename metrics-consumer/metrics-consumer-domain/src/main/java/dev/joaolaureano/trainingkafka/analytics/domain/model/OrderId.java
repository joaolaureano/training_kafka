package dev.joaolaureano.trainingkafka.analytics.domain.model;

import java.util.UUID;

/** Identidade de um {@link OrderRecord} no ledger. */
public record OrderId(UUID value) {

    public OrderId {
        if (value == null) {
            throw new InvalidValueException("orderId é obrigatório");
        }
    }

    public static OrderId of(String raw) {
        try {
            return new OrderId(UUID.fromString(raw));
        } catch (IllegalArgumentException | NullPointerException malformed) {
            throw new InvalidValueException("orderId não é um UUID válido: " + raw);
        }
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
