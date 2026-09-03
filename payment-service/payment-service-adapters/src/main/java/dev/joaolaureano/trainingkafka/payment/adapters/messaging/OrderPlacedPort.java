package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

public interface OrderPlacedPort {
    void handle(OrderPlacedMessage message);
}
