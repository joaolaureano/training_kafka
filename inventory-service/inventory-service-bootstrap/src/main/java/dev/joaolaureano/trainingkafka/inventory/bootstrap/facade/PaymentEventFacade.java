package dev.joaolaureano.trainingkafka.inventory.bootstrap.facade;

import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.PaymentEventMessage;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.PaymentEventPort;
import dev.joaolaureano.trainingkafka.inventory.application.ReleaseStockForOrder;

/**
 * Traduz o desfecho do pagamento na decisão deste contexto.
 *
 * Dos três tipos que trafegam em {@code payment-events}, só dois liberam estoque:
 * o pagamento que falhou e o que foi cancelado por fraude. {@code PaymentApproved}
 * é ignorado de propósito — a reserva vira venda e continua consumida.
 *
 * O filtro mora aqui, e não no listener, porque é uma decisão sobre o SIGNIFICADO
 * da mensagem, não sobre o transporte.
 */
public final class PaymentEventFacade implements PaymentEventPort {

    private static final String PAYMENT_FAILED = "PaymentFailed";
    private static final String PAYMENT_CANCELLED = "PaymentCancelled";

    private final ReleaseStockForOrder releaseStock;

    public PaymentEventFacade(ReleaseStockForOrder releaseStock) {
        this.releaseStock = releaseStock;
    }

    @Override
    public void handle(PaymentEventMessage message) {
        String eventType = message.eventType();
        if (!PAYMENT_FAILED.equals(eventType) && !PAYMENT_CANCELLED.equals(eventType)) {
            return;
        }
        String reason = message.reason() == null ? eventType : eventType + ": " + message.reason();
        releaseStock.handle(message.orderId(), message.customerId(), message.amount(),
                message.correlationId(), reason);
    }
}
