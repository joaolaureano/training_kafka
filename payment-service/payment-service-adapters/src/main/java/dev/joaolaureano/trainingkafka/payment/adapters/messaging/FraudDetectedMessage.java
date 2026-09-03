package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import java.math.BigDecimal;
import java.util.List;

/**
 * O contrato de {@code fraud-events} visto deste lado.
 *
 * Duplicado de propósito: compartilhar a classe do fraud-service colaria os dois
 * bounded contexts pelo classpath, e uma refatoração interna lá quebraria a
 * compilação aqui.
 */
public record FraudDetectedMessage(
        String eventId,
        String correlationId,
        String customerId,
        int ordersInWindow,
        long windowSeconds,
        List<FraudulentOrder> orders,
        String occurredAt) {

    public record FraudulentOrder(String orderId, BigDecimal amount) {
    }
}
