package dev.joaolaureano.trainingkafka.orders.domain.model;

import java.util.UUID;

/** Identidade do agregado Order. */
public record OrderId(UUID value) {

    public OrderId {
        if (value == null) {
            throw new InvalidOrderException("orderId é obrigatório");
        }
    }

    public static OrderId generate() {
        return new OrderId(UUID.randomUUID());
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
