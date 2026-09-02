package dev.joaolaureano.trainingkafka.audit.domain.model;

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
public record AuditFilter(AuditLevel minimumLevel, ApplicationName app, TimeRange range) {

    public static AuditFilter all() {
        return new AuditFilter(null, null, null);
    }

    public AuditFilter withMinimumLevel(AuditLevel level) {
        return new AuditFilter(level, app, range);
    }

    public AuditFilter withApp(ApplicationName application) {
        return new AuditFilter(minimumLevel, application, range);
    }

    public AuditFilter withRange(TimeRange timeRange) {
        return new AuditFilter(minimumLevel, app, timeRange);
    }

    public boolean acceptsLevel(AuditLevel candidate) {
        return minimumLevel == null || candidate.isAtLeast(minimumLevel);
    }

    public boolean acceptsApp(ApplicationName candidate) {
        return app == null || app.equals(candidate);
    }

    public boolean acceptsMoment(Instant moment) {
        return range == null || range.contains(moment);
    }

    public Optional<AuditLevel> minimumLevelOrEmpty() {
        return Optional.ofNullable(minimumLevel);
    }

    public Optional<ApplicationName> appOrEmpty() {
        return Optional.ofNullable(app);
    }

    public Optional<TimeRange> rangeOrEmpty() {
        return Optional.ofNullable(range);
    }
}
