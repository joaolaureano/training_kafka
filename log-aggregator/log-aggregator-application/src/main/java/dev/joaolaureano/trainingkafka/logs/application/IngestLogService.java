package dev.joaolaureano.trainingkafka.logs.application;

import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;
import dev.joaolaureano.trainingkafka.logs.domain.port.LogRepository;

import java.util.Objects;

/**
 * Caso de uso: absorver um registro vindo do tópico.
 *
 * É fino porque não há nada para decidir: um log que chegou é um log que
 * aconteceu. Toda a validação já foi feita na construção do {@link LogEntry},
 * e onde ele será guardado é decisão de wiring.
 */
public class IngestLogService {

    private final LogRepository repository;

    public IngestLogService(LogRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public void ingest(LogEntry entry) {
        repository.save(entry);
    }
}
