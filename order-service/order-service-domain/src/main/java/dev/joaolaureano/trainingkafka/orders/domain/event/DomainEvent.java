package dev.joaolaureano.trainingkafka.orders.domain.event;

import java.time.Instant;

/**
 * Um fato que aconteceu no domínio.
 *
 * O domínio não sabe — e não deve saber — que isso vira uma mensagem JSON num
 * tópico Kafka. Para ele, é só um fato registrado.
 */
public interface DomainEvent {

    Instant occurredAt();
}
