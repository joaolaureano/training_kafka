package dev.joaolaureano.trainingkafka.audit.domain.model;

import java.time.Instant;
import java.util.Map;

/**
 * Um registro de log estruturado.
 *
 * É um value object, não uma entidade: um log não tem identidade nem ciclo de
 * vida — ele aconteceu, e pronto. Dois registros com os mesmos valores SÃO o
 * mesmo registro. Por isso também é imutável: reescrever um log seria reescrever
 * o passado.
 */
public record AuditEvent(
        AuditLevel level,
        Instant occurredAt,
        ApplicationName app,
        String action,
        Map<String, String> context
) {

    public AuditEvent {
        if (level == null) {
            throw new InvalidAuditException("level é obrigatório");
        }
        if (occurredAt == null) {
            throw new InvalidAuditException("occurredAt é obrigatório");
        }
        if (app == null) {
            throw new InvalidAuditException("app é obrigatório");
        }
        if (action == null || action.isBlank()) {
            throw new InvalidAuditException("action é obrigatória");
        }
        context = Map.copyOf(context == null ? Map.of() : context);
    }

    /** Atende ao filtro? A decisão é do próprio registro, não de quem consulta. */
    public boolean matches(AuditFilter filter) {
        return filter.acceptsLevel(level)
                && filter.acceptsApp(app)
                && filter.acceptsMoment(occurredAt);
    }
}
