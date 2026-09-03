package dev.joaolaureano.trainingkafka.payment.domain.event;

import java.time.Instant;

public interface DomainEvent {
    Instant occurredAt();
}
