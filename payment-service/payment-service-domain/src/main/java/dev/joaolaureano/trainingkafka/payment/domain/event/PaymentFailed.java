package dev.joaolaureano.trainingkafka.payment.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentFailed(String paymentId, String orderId, String customerId,
                            BigDecimal amount, Instant occurredAt, String reason,
                            String correlationId) implements DomainEvent {
}
