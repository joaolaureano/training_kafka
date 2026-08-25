package dev.joaolaureano.trainingkafka.analytics.domain.model;

import java.time.Duration;
import java.time.Instant;

/** Um intervalo fechado de tempo, usado como parâmetro das consultas de faturamento. */
public record TimeRange(Instant start, Instant end) {

    public TimeRange {
        if (start == null || end == null) {
            throw new InvalidValueException("start e end são obrigatórios");
        }
        if (start.isAfter(end)) {
            throw new InvalidValueException("start não pode ser posterior a end");
        }
    }

    public static TimeRange lastOf(Duration duration, Instant now) {
        return new TimeRange(now.minus(duration), now);
    }

    public boolean contains(Instant moment) {
        return !moment.isBefore(start) && !moment.isAfter(end);
    }

    public Duration duration() {
        return Duration.between(start, end);
    }
}
