package dev.joaolaureano.trainingkafka.audit.bootstrap.facade;

import dev.joaolaureano.trainingkafka.audit.adapters.web.AuditQueryPort;
import dev.joaolaureano.trainingkafka.audit.application.AuditQueryService;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;

import java.util.List;
import java.util.Objects;

/** Liga o controller de consulta ao caso de uso de leitura. */
public class AuditQueryFacade implements AuditQueryPort {

    private final AuditQueryService queries;

    public AuditQueryFacade(AuditQueryService queries) {
        this.queries = Objects.requireNonNull(queries);
    }

    @Override
    public List<AuditEvent> query(AuditFilter filter, int limit) {
        return queries.query(filter, limit);
    }
}
