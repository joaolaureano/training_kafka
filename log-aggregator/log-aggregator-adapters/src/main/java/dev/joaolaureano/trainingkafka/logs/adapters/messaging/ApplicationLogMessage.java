package dev.joaolaureano.trainingkafka.logs.adapters.messaging;

import java.util.Map;

/**
 * O JSON como ele chega do tópico "application-logs".
 *
 * Cópia própria do contrato, como no App B — os três serviços não compartilham
 * classe de evento, de propósito.
 */
public record ApplicationLogMessage(
        String level,
        String timestamp,
        String app,
        String message,
        Map<String, String> context
) {
}
