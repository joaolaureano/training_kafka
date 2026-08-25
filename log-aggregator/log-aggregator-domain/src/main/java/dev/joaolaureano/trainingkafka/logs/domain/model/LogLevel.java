package dev.joaolaureano.trainingkafka.logs.domain.model;

import java.util.Arrays;

/** Severidade de um registro. */
public enum LogLevel {

    DEBUG, INFO, WARN, ERROR;

    /** Tolerante a caixa, intolerante a lixo. */
    public static LogLevel parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new InvalidLogException("level é obrigatório");
        }
        return Arrays.stream(values())
                .filter(level -> level.name().equalsIgnoreCase(raw.trim()))
                .findFirst()
                .orElseThrow(() -> new InvalidLogException("level desconhecido: " + raw));
    }

    public boolean isAtLeast(LogLevel threshold) {
        return this.ordinal() >= threshold.ordinal();
    }
}
