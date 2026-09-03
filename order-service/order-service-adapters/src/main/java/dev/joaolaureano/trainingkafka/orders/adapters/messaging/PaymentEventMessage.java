package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

public record PaymentEventMessage(String eventType, String eventId, String correlationId,
                                  String paymentId, String orderId, String customerId,
                                  java.math.BigDecimal amount, String occurredAt, String reason,
                                  boolean refunded) {
}
