package dev.joaolaureano.trainingkafka.audit.adapters.messaging;

import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;

/**
 * O que o listener precisa que alguém faça com o registro que ele traduziu.
 *
 * Declarada do lado do consumidor: o adapter compila conhecendo só o domínio, e
 * quem atende — o caso de uso de aplicação, via facade — é decisão do bootstrap.
 */
public interface IngestAuditPort {

    void ingest(AuditEvent entry);
}
