package dev.joaolaureano.trainingkafka.logs.adapters.messaging;

import dev.joaolaureano.trainingkafka.logs.domain.model.ApplicationName;
import dev.joaolaureano.trainingkafka.logs.domain.model.InvalidLogException;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogLevel;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

/** Anticorruption layer: mensagem de fio para registro de domínio. */
public final class LogEntryTranslator {

    private LogEntryTranslator() {
    }

    public static LogEntry toDomain(ApplicationLogMessage message) {
        if (message == null) {
            throw new InvalidLogException("mensagem vazia no tópico de logs");
        }
        return new LogEntry(
                LogLevel.parse(message.level()),
                parseInstant(message.timestamp()),
                new ApplicationName(message.app()),
                message.message(),
                message.context() == null ? Map.of() : message.context());
    }

    private static Instant parseInstant(String timestamp) {
        if (timestamp == null) {
            throw new InvalidLogException("timestamp é obrigatório");
        }
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException malformed) {
            throw new InvalidLogException("timestamp não é um instante ISO-8601: " + timestamp);
        }
    }
}
