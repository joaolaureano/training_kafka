package dev.joaolaureano.trainingkafka.logs.application;

import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogFilter;
import dev.joaolaureano.trainingkafka.logs.domain.port.LogRepository;

import java.util.List;
import java.util.Objects;

/** Lado de leitura da agregação de logs. */
public class LogQueryService {

    private static final int MAX_LIMIT = 1000;

    private final LogRepository repository;

    public LogQueryService(LogRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    /**
     * Teto no limite para que uma consulta descuidada não tente materializar o
     * arquivo inteiro em memória.
     */
    public List<LogEntry> query(LogFilter filter, int limit) {
        return repository.query(filter, Math.clamp(limit, 1, MAX_LIMIT));
    }
}
