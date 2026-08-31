package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import dev.joaolaureano.trainingkafka.analytics.domain.event.OrderPlaced;

/**
 * O que o listener precisa que alguém faça com o evento que ele traduziu.
 *
 * Declarada do lado do consumidor: o adapter compila conhecendo só o domínio, e
 * quem atende — o orquestrador de aplicação, via facade — é decisão do bootstrap.
 */
public interface OrderPlacedPort {

    void handle(OrderPlaced event);
}
