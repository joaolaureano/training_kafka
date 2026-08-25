package dev.joaolaureano.trainingkafka.logs.domain.model;

import java.time.Instant;
import java.util.Optional;

/**
 * Critério de busca. Todos os campos são opcionais — ausente significa "não filtre
 * por isso", e não "filtre por nulo".
 *
 * O {@code minimumLevel} filtra por severidade MÍNIMA, não por igualdade: pedir
 * WARN traz WARN e ERROR. É o comportamento que alguém investigando um incidente
 * espera, e evita ter que consultar duas vezes.
 */
public record LogFilter(LogLevel minimumLevel, ApplicationName app, TimeRange range) {

    public static LogFilter all() {
        return new LogFilter(null, null, null);
    }

    public LogFilter withMinimumLevel(LogLevel level) {
        return new LogFilter(level, app, range);
    }

    public LogFilter withApp(ApplicationName application) {
        return new LogFilter(minimumLevel, application, range);
    }

    public LogFilter withRange(TimeRange timeRange) {
        return new LogFilter(minimumLevel, app, timeRange);
    }

    public boolean acceptsLevel(LogLevel candidate) {
        return minimumLevel == null || candidate.isAtLeast(minimumLevel);
    }

    public boolean acceptsApp(ApplicationName candidate) {
        return app == null || app.equals(candidate);
    }

    public boolean acceptsMoment(Instant moment) {
        return range == null || range.contains(moment);
    }

    public Optional<LogLevel> minimumLevelOrEmpty() {
        return Optional.ofNullable(minimumLevel);
    }

    public Optional<ApplicationName> appOrEmpty() {
        return Optional.ofNullable(app);
    }

    public Optional<TimeRange> rangeOrEmpty() {
        return Optional.ofNullable(range);
    }
}
