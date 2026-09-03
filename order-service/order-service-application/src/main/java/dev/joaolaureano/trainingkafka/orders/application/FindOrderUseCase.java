package dev.joaolaureano.trainingkafka.orders.application;

import java.util.Optional;

public interface FindOrderUseCase {

    Optional<OrderView> byId(String orderId);
}
