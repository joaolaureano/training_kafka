package dev.joaolaureano.trainingkafka.orders.domain.port;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;

import java.util.List;
import java.util.Optional;

/**
 * O estado do pedido e os fatos que ele ainda deve ao mundo.
 *
 * As duas coisas cabem no mesmo Port de propósito: {@link #save(Order, List)} é a
 * promessa de que gravar o pedido e registrar seus eventos acontece numa
 * transação só. É essa promessa que fecha a janela entre "o pedido existe" e "o
 * mundo soube dele" — a publicação em si vira responsabilidade de quem drena o
 * registro depois (o outbox), não do caso de uso.
 */
public interface OrderRepository {

    Optional<Order> findById(OrderId orderId);

    /** Grava o pedido e enfileira seus eventos atomicamente. */
    void save(Order order, List<DomainEvent> events);

    /** Grava só a mudança de estado — usado quando a transição não gera evento novo. */
    void save(Order order);
}
