package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import dev.joaolaureano.trainingkafka.analytics.domain.event.OrderPlaced;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.InvalidValueException;
import dev.joaolaureano.trainingkafka.analytics.domain.model.Money;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.Quantity;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * A Anticorruption Layer, em uma classe.
 *
 * Traduz a mensagem de fio para o evento de domínio DESTE contexto. É o único
 * ponto do App B que conhece o formato publicado pelo App A — daqui para dentro,
 * só existe o vocabulário de análise.
 *
 * Note que a tradução também é uma barreira de qualidade: um payload corrompido
 * é recusado aqui, com {@link InvalidValueException}, e nunca chega a tocar num
 * agregado.
 */
public final class OrderPlacedTranslator {

    private OrderPlacedTranslator() {
    }

    public static OrderPlaced toDomainEvent(OrderPlacedMessage message) {
        if (message == null) {
            throw new InvalidValueException("mensagem vazia no tópico de pedidos");
        }
        return new OrderPlaced(
                OrderId.of(message.orderId()),
                new CustomerId(message.customerId()),
                new ProductId(message.product()),
                toQuantity(message.quantity()),
                new Money(message.amount()),
                toInstant(message.occurredAt()));
    }

    private static Quantity toQuantity(Integer quantity) {
        if (quantity == null) {
            throw new InvalidValueException("quantity é obrigatório");
        }
        return new Quantity(quantity);
    }

    private static Instant toInstant(String occurredAt) {
        if (occurredAt == null) {
            throw new InvalidValueException("occurredAt é obrigatório");
        }
        try {
            return Instant.parse(occurredAt);
        } catch (DateTimeParseException malformed) {
            throw new InvalidValueException("occurredAt não é um instante ISO-8601: " + occurredAt);
        }
    }
}
