package dev.joaolaureano.trainingkafka.audit.domain.model;

import java.time.Instant;

/** Intervalo fechado usado para filtrar registros por período. */
public record TimeRange(Instant start, Instant end) {

    public TimeRange {
        if (start == null || end == null) {
            throw new InvalidAuditException("start e end são obrigatórios");
        }
        if (start.isAfter(end)) {
            throw new InvalidAuditException("start não pode ser posterior a end");
        }
    }

    public boolean contains(Instant moment) {
        return !moment.isBefore(start) && !moment.isAfter(end);
    }
}
