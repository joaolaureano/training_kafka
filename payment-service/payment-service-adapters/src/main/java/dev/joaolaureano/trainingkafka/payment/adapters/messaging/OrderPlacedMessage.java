package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import java.math.BigDecimal;

public record OrderPlacedMessage(String orderId, String customerId, String product,
                                 int quantity, BigDecimal amount, String occurredAt,
                                 String eventId, String correlationId) {
}
