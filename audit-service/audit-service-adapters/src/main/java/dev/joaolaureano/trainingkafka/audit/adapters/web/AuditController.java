package dev.joaolaureano.trainingkafka.audit.adapters.web;

import dev.joaolaureano.trainingkafka.audit.domain.model.ApplicationName;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditLevel;
import dev.joaolaureano.trainingkafka.audit.domain.model.TimeRange;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Consulta de logs agregados.
 *
 * Exemplo: {@code GET /audit-events?level=WARN&app=metrics-consumer&limit=20}
 */
@RestController
@RequestMapping("/audit-events")
public class AuditController {

    private final AuditQueryPort queries;

    public AuditController(AuditQueryPort queries) {
        this.queries = queries;
    }

    @GetMapping
    public List<AuditEventView> query(
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String app,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "50") int limit) {

        AuditFilter filter = AuditFilter.all();
        if (level != null && !level.isBlank()) {
            filter = filter.withMinimumLevel(AuditLevel.parse(level));
        }
        if (app != null && !app.isBlank()) {
            filter = filter.withApp(new ApplicationName(app));
        }
        if (from != null && to != null) {
            filter = filter.withRange(new TimeRange(Instant.parse(from), Instant.parse(to)));
        }

        return queries.query(filter, limit).stream().map(AuditEventView::from).toList();
    }

    public record AuditEventView(String level, String timestamp, String app,
                               String action, Map<String, String> context) {

        static AuditEventView from(AuditEvent entry) {
            return new AuditEventView(
                    entry.level().name(),
                    entry.occurredAt().toString(),
                    entry.app().value(),
                    entry.action(),
                    entry.context());
        }
    }
}
