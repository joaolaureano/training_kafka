package dev.joaolaureano.trainingkafka.inventory.domain.event;

/**
 * Marcador dos fatos que este contexto produz.
 *
 * Sem campo comum e sem classe base: um evento é um fato, e o que todos têm em
 * comum é apenas serem publicáveis. A tradução para o fio mora no adapter.
 */
public interface DomainEvent {
}
