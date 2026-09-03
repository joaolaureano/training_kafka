package dev.joaolaureano.trainingkafka.payment.bootstrap.facade;

import dev.joaolaureano.trainingkafka.payment.adapters.messaging.OrderPlacedMessage;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.OrderPlacedPort;
import dev.joaolaureano.trainingkafka.payment.application.ProcessOrderPayment;

public final class OrderPlacedFacade implements OrderPlacedPort {

    private final ProcessOrderPayment processPayment;

    public OrderPlacedFacade(ProcessOrderPayment processPayment) {
        this.processPayment = processPayment;
    }

    @Override
    public void handle(OrderPlacedMessage message) {
        processPayment.handle(message.orderId(), message.customerId(), message.amount(), message.correlationId());
    }
}
