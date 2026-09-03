package dev.joaolaureano.trainingkafka.fraud.adapters.messaging;

import java.math.BigDecimal;
import java.util.List;

/**
 * O contrato de fio de {@code fraud-events} — a saída OPERACIONAL do detector.
 *
 * Distinta de {@link AuditEventMessage}, que continua indo para {@code
 * audit-events} e é observabilidade. Um alerta é para humano ler; este evento é
 * para o payment-service agir. Misturar os dois faria uma mudança de formato de
 * log quebrar a compensação.
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
