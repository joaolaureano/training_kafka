package dev.joaolaureano.trainingkafka.fraud.adapters.messaging;

import java.util.Map;

public record AuditEventMessage(
        String level,
        String timestamp,
        String app,
        String action,
        Map<String, String> context) {
}
