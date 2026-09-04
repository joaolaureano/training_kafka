package dev.joaolaureano.trainingkafka.inventory.bootstrap.facade;

import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.OrderPlacedMessage;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.OrderPlacedPort;
import dev.joaolaureano.trainingkafka.inventory.application.ReserveStockForOrder;

/** O lado de entrada: o contrato de fio vira os argumentos do caso de uso. */
public final class OrderPlacedFacade implements OrderPlacedPort {

    private final ReserveStockForOrder reserveStock;

    public OrderPlacedFacade(ReserveStockForOrder reserveStock) {
        this.reserveStock = reserveStock;
    }

    @Override
    public void handle(OrderPlacedMessage message) {
        reserveStock.handle(message.orderId(), message.customerId(), message.product(),
                message.quantity(), message.amount(), message.correlationId());
    }
}
