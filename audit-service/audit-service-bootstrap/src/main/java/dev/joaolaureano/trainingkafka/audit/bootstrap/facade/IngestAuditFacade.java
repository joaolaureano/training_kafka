package dev.joaolaureano.trainingkafka.audit.bootstrap.facade;

import dev.joaolaureano.trainingkafka.audit.adapters.messaging.IngestAuditPort;
import dev.joaolaureano.trainingkafka.audit.application.IngestAuditService;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;

import java.util.Objects;

/** Liga o listener Kafka ao caso de uso de ingestão. */
public class IngestAuditFacade implements IngestAuditPort {

    private final IngestAuditService ingest;

    public IngestAuditFacade(IngestAuditService ingest) {
        this.ingest = Objects.requireNonNull(ingest);
    }

    @Override
    public void ingest(AuditEvent entry) {
        ingest.ingest(entry);
    }
}
