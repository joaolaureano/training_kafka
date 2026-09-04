package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * O lado do Order no primeiro elo da Saga: aplica no agregado o que o contexto de
 * Inventory decidiu.
 *
 * Mesma forma de {@link ApplyPaymentResult}, e pelo mesmo motivo: não há chamada
 * de volta nem transação distribuída — o pedido é dono do próprio estado e reage
 * a um fato que já aconteceu. A idempotência mora no agregado.
 */
public final class ApplyStockResult {

    private final OrderRepository orders;

    public ApplyStockResult(OrderRepository orders) {
        this.orders = Objects.requireNonNull(orders);
    }

    /** Unidades separadas: o pedido pode ser cobrado. */
    public void reserved(String orderId) {
        apply(orderId, Order::confirmStock);
    }

    /** Produto inexistente ou sem estoque: o pedido termina aqui, sem cobrança. */
    public void rejected(String orderId) {
        apply(orderId, Order::cancelForOutOfStock);
    }

    /**
     * Carregar, transicionar e gravar acontece dentro do repositório, num passo só.
     *
     * O pedido tem dois escritores concorrentes — os resultados de estoque e de
     * pagamento chegam por tópicos diferentes, em threads diferentes. Um
     * {@code findById} seguido de {@code save} aqui deixaria a janela do lost
     * update, e o pedido ficaria preso num estado que nenhum dos dois eventos
     * pediu, sem exceção e sem DLQ.
     */
    private void apply(String orderId, Consumer<Order> transition) {
        if (!orders.applyTransition(OrderId.parse(orderId), transition)) {
            throw new UnknownOrderException(orderId);
        }
    }
}
