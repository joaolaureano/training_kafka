package dev.joaolaureano.trainingkafka.logs.bootstrap.facade;

import dev.joaolaureano.trainingkafka.logs.adapters.web.LogQueryPort;
import dev.joaolaureano.trainingkafka.logs.application.LogQueryService;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogFilter;

import java.util.List;
import java.util.Objects;

/** Liga o controller de consulta ao caso de uso de leitura. */
public class LogQueryFacade implements LogQueryPort {

    private final LogQueryService queries;

    public LogQueryFacade(LogQueryService queries) {
        this.queries = Objects.requireNonNull(queries);
    }

    @Override
    public List<LogEntry> query(LogFilter filter, int limit) {
        return queries.query(filter, limit);
    }
}
