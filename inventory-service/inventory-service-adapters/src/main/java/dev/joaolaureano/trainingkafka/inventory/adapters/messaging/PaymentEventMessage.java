package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import java.math.BigDecimal;

/**
 * Contrato de fio de {@code payment-events} — a mesma forma que o order-service
 * consome, replicada aqui pelo mesmo motivo de {@link OrderPlacedMessage}.
 */
public record PaymentEventMessage(String eventType, String eventId, String correlationId,
                                  String paymentId, String orderId, String customerId,
                                  BigDecimal amount, String occurredAt, String reason,
                                  boolean refunded) {
}
