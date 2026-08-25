package dev.joaolaureano.trainingkafka.analytics.domain.event;

import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Um cliente concentrou pedidos demais numa janela curta.
 *
 * Emitido pelo próprio agregado {@code CustomerOrderPattern}. O domínio não sabe
 * que isto vira um log JSON num tópico Kafka — para ele, é só um fato registrado.
 */
public record SuspiciousPatternDetected(
        CustomerId customerId,
        int ordersInWindow,
        Duration window,
        List<OrderId> sample,
        Instant occurredAt
) implements DomainEvent {

    public SuspiciousPatternDetected {
        sample = List.copyOf(sample);
    }
}
