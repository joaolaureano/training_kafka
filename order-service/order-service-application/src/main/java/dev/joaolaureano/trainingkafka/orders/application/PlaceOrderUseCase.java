package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;

public interface PlaceOrderUseCase {

    OrderId handle(PlaceOrderCommand command);
}
