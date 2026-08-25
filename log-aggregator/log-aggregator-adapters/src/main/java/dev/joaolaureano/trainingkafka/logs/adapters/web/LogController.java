package dev.joaolaureano.trainingkafka.logs.adapters.web;

import dev.joaolaureano.trainingkafka.logs.application.LogQueryService;
import dev.joaolaureano.trainingkafka.logs.domain.model.ApplicationName;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogFilter;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogLevel;
import dev.joaolaureano.trainingkafka.logs.domain.model.TimeRange;
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
 * Exemplo: {@code GET /logs?level=WARN&app=metrics-consumer&limit=20}
 */
@RestController
@RequestMapping("/logs")
public class LogController {

    private final LogQueryService queries;

    public LogController(LogQueryService queries) {
        this.queries = queries;
    }

    @GetMapping
    public List<LogEntryView> query(
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String app,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "50") int limit) {

        LogFilter filter = LogFilter.all();
        if (level != null && !level.isBlank()) {
            filter = filter.withMinimumLevel(LogLevel.parse(level));
        }
        if (app != null && !app.isBlank()) {
            filter = filter.withApp(new ApplicationName(app));
        }
        if (from != null && to != null) {
            filter = filter.withRange(new TimeRange(Instant.parse(from), Instant.parse(to)));
        }

        return queries.query(filter, limit).stream().map(LogEntryView::from).toList();
    }

    public record LogEntryView(String level, String timestamp, String app,
                               String message, Map<String, String> context) {

        static LogEntryView from(LogEntry entry) {
            return new LogEntryView(
                    entry.level().name(),
                    entry.occurredAt().toString(),
                    entry.app().value(),
                    entry.message(),
                    entry.context());
        }
    }
}
