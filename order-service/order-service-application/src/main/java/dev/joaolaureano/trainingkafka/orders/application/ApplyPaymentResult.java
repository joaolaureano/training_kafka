package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * O lado do Order na Saga: aplica no agregado o que o contexto de Payment decidiu.
 *
 * Não há chamada de volta para o payment-service, nem transação distribuída: o
 * pedido é dono do próprio estado e reage a um fato que já aconteceu. A
 * idempotência mora no agregado — aqui só se traduz o identificador e se persiste
 * o resultado.
 */
public final class ApplyPaymentResult {

    private final OrderRepository orders;

    public ApplyPaymentResult(OrderRepository orders) {
        this.orders = Objects.requireNonNull(orders);
    }

    public void approved(String orderId) {
        apply(orderId, Order::approvePayment);
    }

    public void failed(String orderId) {
        apply(orderId, Order::cancelForPaymentFailure);
    }

    /**
     * O pagamento foi cancelado por fraude detectada depois do fato.
     *
     * Chega como PaymentCancelled, e não direto do fraud-service: o pedido só muda
     * de estado por decisão do contexto que é dono do dinheiro. Assim não há
     * corrida entre cancelar o pedido e estornar a cobrança.
     */
    public void cancelledForFraud(String orderId) {
        apply(orderId, Order::cancelForFraud);
    }

    private void apply(String orderId, Consumer<Order> transition) {
        OrderId id = OrderId.parse(orderId);
        Order order = orders.findById(id).orElseThrow(() -> new UnknownOrderException(orderId));
        transition.accept(order);
        orders.save(order);
    }
}
