package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import java.math.BigDecimal;

public record PaymentEventMessage(String eventType, String eventId, String correlationId,
                                  String paymentId, String orderId, String customerId,
                                  BigDecimal amount, String occurredAt, String reason,
                                  boolean refunded) {
}
