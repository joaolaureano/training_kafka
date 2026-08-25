package dev.joaolaureano.trainingkafka.analytics.domain.model;

import java.time.Instant;

/**
 * Uma entrada na janela de pedidos recentes de um cliente.
 *
 * Vive dentro do agregado {@link CustomerOrderPattern} e não faz sentido fora
 * dele — não tem identidade própria, é definido pelos seus valores.
 */
public record OrderPlacement(OrderId orderId, Instant occurredAt, Money amount) {

    public OrderPlacement {
        if (orderId == null) {
            throw new InvalidValueException("orderId é obrigatório");
        }
        if (occurredAt == null) {
            throw new InvalidValueException("occurredAt é obrigatório");
        }
        if (amount == null) {
            throw new InvalidValueException("amount é obrigatório");
        }
    }
}
