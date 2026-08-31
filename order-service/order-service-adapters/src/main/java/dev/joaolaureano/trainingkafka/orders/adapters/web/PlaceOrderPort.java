package dev.joaolaureano.trainingkafka.orders.adapters.web;

import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;

/**
 * O que o adapter web precisa que alguém faça por ele.
 *
 * A interface é declarada AQUI, do lado de quem consome, e não do lado de quem
 * implementa: assim o controller compila sabendo apenas do domínio, e quem
 * atende a chamada — hoje o caso de uso de aplicação, via facade montada no
 * bootstrap — é uma decisão tomada em outro módulo.
 */
public interface PlaceOrderPort {

    OrderId place(PlaceOrderRequest request);
}
