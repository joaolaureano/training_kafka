package dev.joaolaureano.trainingkafka.audit.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada: consome o tópico "audit-events".
 *
 * Sem try/catch: a falha sobe para o error handler do container, que retenta ou
 * publica na DLQ conforme {@code kafka.dlq.*}.
 */
@Component
public class AuditEventListener {

    private final IngestAuditPort ingest;

    public AuditEventListener(IngestAuditPort ingest) {
        this.ingest = ingest;
    }

    @KafkaListener(topics = Topics.AUDIT_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onAuditEvent(AuditEventMessage message) {
        ingest.ingest(AuditEventTranslator.toDomain(message));
    }
}
