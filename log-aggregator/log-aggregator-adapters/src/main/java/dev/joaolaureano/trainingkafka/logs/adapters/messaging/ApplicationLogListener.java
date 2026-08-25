package dev.joaolaureano.trainingkafka.logs.adapters.messaging;

import dev.joaolaureano.trainingkafka.logs.application.IngestLogService;
import dev.joaolaureano.trainingkafka.logs.domain.model.InvalidLogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Adapter de entrada: consome o tópico "application-logs". */
@Component
public class ApplicationLogListener {

    private static final Logger log = LoggerFactory.getLogger(ApplicationLogListener.class);

    private final IngestLogService ingest;

    public ApplicationLogListener(IngestLogService ingest) {
        this.ingest = ingest;
    }

    @KafkaListener(topics = "application-logs", groupId = "${spring.kafka.consumer.group-id}")
    public void onApplicationLog(ApplicationLogMessage message) {
        try {
            ingest.ingest(LogEntryTranslator.toDomain(message));
        } catch (InvalidLogException malformed) {
            // Descartar e seguir: relançar faria o Kafka reentregar para sempre
            // um payload que nunca vai ser aceito, travando a partição.
            log.warn("Registro descartado por payload inválido: {}", malformed.getMessage());
        }
    }
}
