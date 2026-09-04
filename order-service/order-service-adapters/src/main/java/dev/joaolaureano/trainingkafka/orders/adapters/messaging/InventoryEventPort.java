package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

public interface InventoryEventPort {
    void handle(InventoryEventMessage message);
}
