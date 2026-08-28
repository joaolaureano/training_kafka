package dev.joaolaureano.trainingkafka.logs.adapters.messaging;

import dev.joaolaureano.trainingkafka.logs.application.IngestLogService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada: consome o tópico "application-logs".
 *
 * Sem try/catch: a falha sobe para o error handler do container, que retenta ou
 * publica na DLQ conforme {@code kafka.dlq.*}.
 */
@Component
public class ApplicationLogListener {

    private final IngestLogService ingest;

    public ApplicationLogListener(IngestLogService ingest) {
        this.ingest = ingest;
    }

    @KafkaListener(topics = Topics.APPLICATION_LOGS, groupId = "${spring.kafka.consumer.group-id}")
    public void onApplicationLog(ApplicationLogMessage message) {
        ingest.ingest(LogEntryTranslator.toDomain(message));
    }
}
