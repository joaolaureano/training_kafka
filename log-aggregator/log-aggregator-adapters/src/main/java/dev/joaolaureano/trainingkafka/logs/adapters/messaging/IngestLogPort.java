package dev.joaolaureano.trainingkafka.logs.adapters.messaging;

import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;

/**
 * O que o listener precisa que alguém faça com o registro que ele traduziu.
 *
 * Declarada do lado do consumidor: o adapter compila conhecendo só o domínio, e
 * quem atende — o caso de uso de aplicação, via facade — é decisão do bootstrap.
 */
public interface IngestLogPort {

    void ingest(LogEntry entry);
}
