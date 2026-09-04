package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

public interface StockReservedPort {
    void handle(InventoryEventMessage message);
}
