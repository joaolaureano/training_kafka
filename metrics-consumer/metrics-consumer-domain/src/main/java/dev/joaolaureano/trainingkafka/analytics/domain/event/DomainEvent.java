package dev.joaolaureano.trainingkafka.analytics.domain.event;

import java.time.Instant;

/** Um fato ocorrido no contexto de análise. */
public interface DomainEvent {

    Instant occurredAt();
}
