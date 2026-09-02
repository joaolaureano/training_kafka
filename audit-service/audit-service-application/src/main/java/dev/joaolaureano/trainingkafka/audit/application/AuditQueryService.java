package dev.joaolaureano.trainingkafka.audit.application;

import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;
import dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository;

import java.util.List;
import java.util.Objects;

/** Lado de leitura da agregação de logs. */
public class AuditQueryService {

    private static final int MAX_LIMIT = 1000;

    private final AuditRepository repository;

    public AuditQueryService(AuditRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    /**
     * Teto no limite para que uma consulta descuidada não tente materializar o
     * arquivo inteiro em memória.
     */
    public List<AuditEvent> query(AuditFilter filter, int limit) {
        return repository.query(filter, Math.clamp(limit, 1, MAX_LIMIT));
    }
}
