package dev.joaolaureano.trainingkafka.logs.domain.model;

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
public record LogEntry(
        LogLevel level,
        Instant occurredAt,
        ApplicationName app,
        String message,
        Map<String, String> context
) {

    public LogEntry {
        if (level == null) {
            throw new InvalidLogException("level é obrigatório");
        }
        if (occurredAt == null) {
            throw new InvalidLogException("occurredAt é obrigatório");
        }
        if (app == null) {
            throw new InvalidLogException("app é obrigatório");
        }
        if (message == null || message.isBlank()) {
            throw new InvalidLogException("message é obrigatória");
        }
        context = Map.copyOf(context == null ? Map.of() : context);
    }

    /** Atende ao filtro? A decisão é do próprio registro, não de quem consulta. */
    public boolean matches(LogFilter filter) {
        return filter.acceptsLevel(level)
                && filter.acceptsApp(app)
                && filter.acceptsMoment(occurredAt);
    }
}
