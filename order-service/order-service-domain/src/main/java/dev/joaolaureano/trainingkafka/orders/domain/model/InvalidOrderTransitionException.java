package dev.joaolaureano.trainingkafka.orders.domain.model;

/**
 * Uma transição de estado que o ciclo de vida do pedido não admite.
 *
 * Não confundir com duplicata: reaplicar o MESMO resultado de pagamento é
 * esperado num consumidor at-least-once e não levanta nada. O que esta exceção
 * marca é a contradição — aprovar um pedido já cancelado, cancelar um já pago —
 * que só pode significar dois resultados conflitantes para o mesmo pedido.
 * Como é permanente, o error handler a manda direto para a DLQ, sem retentar.
 */
public class InvalidOrderTransitionException extends RuntimeException {

    public InvalidOrderTransitionException(OrderId orderId, OrderStatus from, OrderStatus to) {
        super("order " + orderId + " cannot move from " + from + " to " + to);
    }
}
