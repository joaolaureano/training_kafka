package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import java.util.Map;

/** Contrato JSON do tópico "application-logs" — o mesmo formato que o App A publica. */
public record ApplicationLogMessage(
        String level,
        String timestamp,
        String app,
        String message,
        Map<String, String> context
) {
}
