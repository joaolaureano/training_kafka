package dev.joaolaureano.trainingkafka.orders.adapters.persistence;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;

/**
 * Traduz um fato do domínio na linha de outbox correspondente.
 *
 * A implementação vive no pacote de messaging, junto do contrato JSON e do nome
 * do tópico — o repositório só precisa saber que existe alguém capaz de fazer a
 * tradução, não como o Kafka nomeia as coisas.
 */
public interface OutboxTranslator {

    OutboxRecord translate(DomainEvent event);
}
