package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

public interface PaymentEventPort {
    void handle(PaymentEventMessage message);
}
