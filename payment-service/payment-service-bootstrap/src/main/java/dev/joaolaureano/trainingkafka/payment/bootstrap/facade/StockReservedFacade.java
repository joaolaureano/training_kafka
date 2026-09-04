package dev.joaolaureano.trainingkafka.payment.bootstrap.facade;

import dev.joaolaureano.trainingkafka.payment.adapters.messaging.InventoryEventMessage;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.StockReservedPort;
import dev.joaolaureano.trainingkafka.payment.application.ProcessOrderPayment;

/**
 * Só {@code StockReserved} cobra.
 *
 * {@code StockRejected} encerra o pedido sem cobrança, e {@code StockReleased} é
 * consequência de um pagamento que este contexto já decidiu — reagir a qualquer
 * um dos dois seria cobrar por engano. O filtro mora aqui, e não no listener,
 * porque é uma decisão sobre o SIGNIFICADO da mensagem, não sobre o transporte.
 */
public final class StockReservedFacade implements StockReservedPort {

    private static final String STOCK_RESERVED = "StockReserved";

    private final ProcessOrderPayment processPayment;

    public StockReservedFacade(ProcessOrderPayment processPayment) {
        this.processPayment = processPayment;
    }

    @Override
    public void handle(InventoryEventMessage message) {
        if (!STOCK_RESERVED.equals(message.eventType())) {
            return;
        }
        processPayment.handle(message.orderId(), message.customerId(), message.amount(),
                message.correlationId());
    }
}
