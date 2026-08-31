package dev.joaolaureano.trainingkafka.logs.bootstrap.facade;

import dev.joaolaureano.trainingkafka.logs.adapters.messaging.IngestLogPort;
import dev.joaolaureano.trainingkafka.logs.application.IngestLogService;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;

import java.util.Objects;

/** Liga o listener Kafka ao caso de uso de ingestão. */
public class IngestLogFacade implements IngestLogPort {

    private final IngestLogService ingest;

    public IngestLogFacade(IngestLogService ingest) {
        this.ingest = Objects.requireNonNull(ingest);
    }

    @Override
    public void ingest(LogEntry entry) {
        ingest.ingest(entry);
    }
}
