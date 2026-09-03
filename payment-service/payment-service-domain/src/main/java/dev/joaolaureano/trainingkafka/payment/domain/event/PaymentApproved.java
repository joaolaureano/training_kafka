package dev.joaolaureano.trainingkafka.payment.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentApproved(String paymentId, String orderId, String customerId,
                              BigDecimal amount, Instant occurredAt, String correlationId)
        implements DomainEvent {
}
