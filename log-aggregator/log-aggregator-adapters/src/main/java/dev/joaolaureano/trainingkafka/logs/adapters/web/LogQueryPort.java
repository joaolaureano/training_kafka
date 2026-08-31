package dev.joaolaureano.trainingkafka.logs.adapters.web;

import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogFilter;

import java.util.List;

/**
 * O que o adapter web precisa para responder {@code GET /logs}.
 *
 * Declarada do lado do consumidor: o controller compila conhecendo só o domínio,
 * e quem atende — o caso de uso de aplicação, via facade — é decisão do bootstrap.
 */
public interface LogQueryPort {

    List<LogEntry> query(LogFilter filter, int limit);
}
