package dev.joaolaureano.trainingkafka.orders.adapters.web;

import java.util.Optional;

/**
 * O que o controller precisa que alguém faça por ele para responder uma consulta.
 *
 * Declarado aqui, do lado de quem consome, como o {@link PlaceOrderPort}.
 */
public interface FindOrderPort {

    Optional<OrderResponse> byId(String orderId);
}
