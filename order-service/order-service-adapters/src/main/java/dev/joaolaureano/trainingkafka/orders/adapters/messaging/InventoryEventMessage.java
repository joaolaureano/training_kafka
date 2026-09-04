package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import java.math.BigDecimal;

/**
 * Contrato de fio de {@code inventory-events}, na versão que ESTE contexto precisa.
 *
 * Cópia deliberada do record que o inventory-service publica, e não uma classe
 * compartilhada — o mesmo motivo pelo qual {@link PaymentEventMessage} também é
 * uma cópia: um módulo comum de contratos faria os bounded contexts evoluírem
 * juntos pelo classpath.
 */
public record InventoryEventMessage(String eventType, String eventId, String correlationId,
                                    String orderId, String customerId, String sku,
                                    int quantity, BigDecimal amount, String reason,
                                    String occurredAt) {
}
