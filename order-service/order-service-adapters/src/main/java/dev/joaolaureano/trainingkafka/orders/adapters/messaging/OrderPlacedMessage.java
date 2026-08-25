package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import dev.joaolaureano.trainingkafka.orders.domain.event.OrderPlaced;

import java.math.BigDecimal;

/**
 * Representação de fio do evento — o contrato JSON publicado no tópico "orders".
 *
 * É deliberadamente uma classe separada de {@link OrderPlaced}. O evento de
 * domínio pode ser refatorado (renomear um campo, extrair um value object) sem
 * quebrar consumidores; e o contrato publicado pode evoluir sem arrastar o
 * domínio junto. Esta classe é a junta que permite os dois se moverem sozinhos.
 */
public record OrderPlacedMessage(
        String orderId,
        String customerId,
        String product,
        int quantity,
        BigDecimal amount,
        String occurredAt
) {

    static OrderPlacedMessage from(OrderPlaced event) {
        return new OrderPlacedMessage(
                event.orderId().toString(),
                event.customerId().value(),
                event.productId().value(),
                event.quantity().value(),
                event.amount().amount(),
                event.occurredAt().toString());
    }
}
