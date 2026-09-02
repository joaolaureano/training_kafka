package dev.joaolaureano.trainingkafka.audit.domain.model;

import java.util.Arrays;

/** Severidade de um registro. */
public enum AuditLevel {

    DEBUG, INFO, WARN, ERROR;

    /** Tolerante a caixa, intolerante a lixo. */
    public static AuditLevel parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new InvalidAuditException("level é obrigatório");
        }
        return Arrays.stream(values())
                .filter(level -> level.name().equalsIgnoreCase(raw.trim()))
                .findFirst()
                .orElseThrow(() -> new InvalidAuditException("level desconhecido: " + raw));
    }

    public boolean isAtLeast(AuditLevel threshold) {
        return this.ordinal() >= threshold.ordinal();
    }
}
