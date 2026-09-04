package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import java.math.BigDecimal;

/**
 * Contrato de fio do tópico {@code orders}, na versão que ESTE contexto precisa.
 *
 * É uma cópia deliberada do record que o order-service publica, e não uma classe
 * compartilhada: um módulo comum de contratos faria os dois bounded contexts
 * evoluírem juntos pelo classpath, que é exatamente o acoplamento que a tradução
 * na fronteira existe para evitar.
 */
public record OrderPlacedMessage(String orderId, String customerId, String product,
                                 int quantity, BigDecimal amount, String occurredAt,
                                 String eventId, String correlationId) {
}
