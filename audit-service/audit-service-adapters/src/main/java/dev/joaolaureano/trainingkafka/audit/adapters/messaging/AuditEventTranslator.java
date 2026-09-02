package dev.joaolaureano.trainingkafka.audit.adapters.messaging;

import dev.joaolaureano.trainingkafka.audit.domain.model.ApplicationName;
import dev.joaolaureano.trainingkafka.audit.domain.model.InvalidAuditException;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditLevel;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

/** Anticorruption layer: mensagem de fio para registro de domínio. */
public final class AuditEventTranslator {

    private AuditEventTranslator() {
    }

    public static AuditEvent toDomain(AuditEventMessage message) {
        if (message == null) {
            throw new InvalidAuditException("mensagem vazia no tópico de logs");
        }
        return new AuditEvent(
                AuditLevel.parse(message.level()),
                parseInstant(message.timestamp()),
                new ApplicationName(message.app()),
                message.action(),
                message.context() == null ? Map.of() : message.context());
    }

    private static Instant parseInstant(String timestamp) {
        if (timestamp == null) {
            throw new InvalidAuditException("timestamp é obrigatório");
        }
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException malformed) {
            throw new InvalidAuditException("timestamp não é um instante ISO-8601: " + timestamp);
        }
    }
}
