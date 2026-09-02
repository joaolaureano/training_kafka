package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import java.util.Map;

/** Contrato JSON do tópico "audit-events" — o mesmo formato que o App A publica. */
public record AuditEventMessage(
        String level,
        String timestamp,
        String app,
        String action,
        Map<String, String> context
) {
}
