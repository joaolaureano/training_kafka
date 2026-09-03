package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderException;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository;

import java.util.Objects;
import java.util.Optional;

/**
 * Caso de uso: consultar em que pé está um pedido.
 *
 * Existe porque o desfecho da Saga é assíncrono. O POST devolve 202 e um
 * identificador; PAID ou CANCELLED chega depois, por evento. Sem uma leitura, o
 * estado final do pedido não é observável de fora — nem por um humano, nem pelo
 * teste de carga.
 *
 * Um id malformado não é erro de servidor: é pergunta sobre um pedido que não
 * pode existir, e a resposta é a mesma de um id válido e desconhecido.
 */
public class FindOrderService implements FindOrderUseCase {

    private final OrderRepository orders;

    public FindOrderService(OrderRepository orders) {
        this.orders = Objects.requireNonNull(orders);
    }

    @Override
    public Optional<OrderView> byId(String orderId) {
        OrderId id;
        try {
            id = OrderId.parse(orderId);
        } catch (InvalidOrderException malformed) {
            return Optional.empty();
        }
        return orders.findById(id).map(FindOrderService::toView);
    }

    private static OrderView toView(Order order) {
        return new OrderView(order.id().toString(), order.status().name(),
                order.customerId().value(), order.productId().value(),
                order.quantity().value(), order.amount().amount(), order.placedAt());
    }
}
