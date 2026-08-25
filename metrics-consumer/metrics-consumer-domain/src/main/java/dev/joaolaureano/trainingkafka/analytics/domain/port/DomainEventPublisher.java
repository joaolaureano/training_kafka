package dev.joaolaureano.trainingkafka.analytics.domain.port;

import dev.joaolaureano.trainingkafka.analytics.domain.event.DomainEvent;

/**
 * Port de saída para os fatos que o domínio quer tornar públicos.
 *
 * O agregado registra o evento; alguém, lá fora, decide que isso vira um log JSON
 * no tópico "application-logs". Essa decisão não está aqui, e não está no domínio.
 */
public interface DomainEventPublisher {

    void publish(DomainEvent event);
}
