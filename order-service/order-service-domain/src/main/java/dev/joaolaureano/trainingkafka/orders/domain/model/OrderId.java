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

    /** Reidrata a identidade vinda de fora — de uma linha do banco ou de uma mensagem. */
    public static OrderId parse(String value) {
        try {
            return new OrderId(UUID.fromString(value));
        } catch (IllegalArgumentException | NullPointerException malformed) {
            throw new InvalidOrderException("orderId inválido: " + value);
        }
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
