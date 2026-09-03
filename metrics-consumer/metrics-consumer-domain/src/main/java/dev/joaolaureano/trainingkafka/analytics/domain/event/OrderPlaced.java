package dev.joaolaureano.trainingkafka.analytics.domain.event;

import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.Money;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.Quantity;

import java.time.Instant;

/**
 * O fato "um pedido foi feito", já no vocabulário DESTE contexto.
 *
 * É uma classe própria do App B, e não a do App A. O adapter de entrada traduz o
 * JSON do tópico para cá — é a Anticorruption Layer. Graças a ela, o App A pode
 * renomear campos internos sem que o modelo de análise sinta.
 */
public record OrderPlaced(
        OrderId orderId,
        CustomerId customerId,
        ProductId productId,
        Quantity quantity,
        Money amount,
        Instant occurredAt
) {
}
