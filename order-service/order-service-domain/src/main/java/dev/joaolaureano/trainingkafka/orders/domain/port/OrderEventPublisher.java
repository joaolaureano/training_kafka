package dev.joaolaureano.trainingkafka.orders.domain.port;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;

/**
 * Port de saída: alguém, lá fora, precisa carregar os fatos do domínio para o mundo.
 *
 * Repare que a assinatura não menciona tópico, chave, partição, serialização nem
 * broker. Trocar Kafka por RabbitMQ, por um webhook ou por uma tabela de outbox
 * não muda uma vírgula desta interface — nem do domínio que a usa.
 */
public interface OrderEventPublisher {

    void publish(DomainEvent event);
}
