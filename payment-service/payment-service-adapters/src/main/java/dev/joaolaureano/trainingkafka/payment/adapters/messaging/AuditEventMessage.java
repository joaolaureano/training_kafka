package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import java.util.Map;

/**
 * O contrato JSON de {@code audit-events}, do jeito que o App C espera ler.
 *
 * Duplicado do produtor original pelo mesmo motivo dos outros contratos: o que
 * une os serviços é o formato no tópico, não uma classe compartilhada.
 */
public record AuditEventMessage(
        String level,
        String timestamp,
        String app,
        String action,
        Map<String, String> context) {
}
