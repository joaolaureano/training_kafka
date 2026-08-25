package dev.joaolaureano.trainingkafka.logs.domain.model;

import java.time.Instant;

/** Intervalo fechado usado para filtrar registros por período. */
public record TimeRange(Instant start, Instant end) {

    public TimeRange {
        if (start == null || end == null) {
            throw new InvalidLogException("start e end são obrigatórios");
        }
        if (start.isAfter(end)) {
            throw new InvalidLogException("start não pode ser posterior a end");
        }
    }

    public boolean contains(Instant moment) {
        return !moment.isBefore(start) && !moment.isAfter(end);
    }
}
