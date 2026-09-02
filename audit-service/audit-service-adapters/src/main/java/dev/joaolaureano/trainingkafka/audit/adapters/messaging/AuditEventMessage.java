package dev.joaolaureano.trainingkafka.audit.adapters.messaging;

import java.util.Map;

/**
 * O JSON como ele chega do tópico "audit-events".
 *
 * Cópia própria do contrato, como no App B — os três serviços não compartilham
 * classe de evento, de propósito.
 */
public record AuditEventMessage(
        String level,
        String timestamp,
        String app,
        String action,
        Map<String, String> context
) {
}
