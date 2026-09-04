package dev.joaolaureano.trainingkafka.orders.bootstrap.facade;

import dev.joaolaureano.trainingkafka.orders.adapters.messaging.InventoryEventMessage;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.InventoryEventPort;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.UnknownInventoryEventException;
import dev.joaolaureano.trainingkafka.orders.application.ApplyStockResult;

/**
 * Traduz o veredito do estoque na transição correspondente do pedido.
 *
 * {@code StockReleased} é ignorado de propósito: a devolução ao estoque acontece
 * porque o pedido JÁ foi cancelado por outro caminho — pagamento recusado ou
 * fraude —, e o cancelamento chega ao Order por {@code payment-events}. Reagir
 * aqui também seria cancelar duas vezes o mesmo pedido, por duas rotas
 * diferentes, e é assim que uma Saga passa a depender da ordem de chegada entre
 * tópicos distintos.
 */
public final class InventoryEventFacade implements InventoryEventPort {

    private static final String STOCK_RESERVED = "StockReserved";
    private static final String STOCK_REJECTED = "StockRejected";
    private static final String STOCK_RELEASED = "StockReleased";

    private final ApplyStockResult applyResult;

    public InventoryEventFacade(ApplyStockResult applyResult) {
        this.applyResult = applyResult;
    }

    @Override
    public void handle(InventoryEventMessage message) {
        switch (message.eventType()) {
            case STOCK_RESERVED -> applyResult.reserved(message.orderId());
            case STOCK_REJECTED -> applyResult.rejected(message.orderId());
            case STOCK_RELEASED -> {
                // Ver o javadoc da classe: quem cancela o pedido é o contexto do dinheiro.
            }
            case null, default -> throw new UnknownInventoryEventException(message.eventType());
        }
    }
}
