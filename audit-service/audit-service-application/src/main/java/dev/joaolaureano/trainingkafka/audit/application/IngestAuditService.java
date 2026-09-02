package dev.joaolaureano.trainingkafka.audit.application;

import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository;

import java.util.Objects;

/**
 * Caso de uso: absorver um registro vindo do tópico.
 *
 * É fino porque não há nada para decidir: um log que chegou é um log que
 * aconteceu. Toda a validação já foi feita na construção do {@link AuditEvent},
 * e onde ele será guardado é decisão de wiring.
 */
public class IngestAuditService {

    private final AuditRepository repository;

    public IngestAuditService(AuditRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public void ingest(AuditEvent entry) {
        repository.save(entry);
    }
}
