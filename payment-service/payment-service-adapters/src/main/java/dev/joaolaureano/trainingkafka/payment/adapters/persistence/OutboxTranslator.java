package dev.joaolaureano.trainingkafka.payment.adapters.persistence;

import dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent;

/**
 * Traduz um fato do domínio na linha de outbox correspondente.
 *
 * A implementação mora no pacote de messaging, junto do contrato JSON e do nome
 * do tópico — o repositório não precisa saber como o Kafka nomeia as coisas.
 */
public interface OutboxTranslator {

    OutboxRecord translate(DomainEvent event);
}
