package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

public interface PaymentEventPort {

    void handle(PaymentEventMessage message);
}
