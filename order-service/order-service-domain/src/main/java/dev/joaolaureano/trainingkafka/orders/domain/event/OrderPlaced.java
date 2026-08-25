package dev.joaolaureano.trainingkafka.orders.domain.event;

import dev.joaolaureano.trainingkafka.orders.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.orders.domain.model.Money;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.orders.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.orders.domain.model.Quantity;

import java.time.Instant;

/** Um pedido foi aceito. É o fato que o contexto de Ordering publica para o mundo. */
public record OrderPlaced(
        OrderId orderId,
        CustomerId customerId,
        ProductId productId,
        Quantity quantity,
        Money amount,
        Instant occurredAt
) implements DomainEvent {
}
