package dev.joaolaureano.trainingkafka.audit.adapters.web;

import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;

import java.util.List;

/**
 * O que o adapter web precisa para responder {@code GET /audit-events}.
 *
 * Declarada do lado do consumidor: o controller compila conhecendo só o domínio,
 * e quem atende — o caso de uso de aplicação, via facade — é decisão do bootstrap.
 */
public interface AuditQueryPort {

    List<AuditEvent> query(AuditFilter filter, int limit);
}
