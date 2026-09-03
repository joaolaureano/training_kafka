package dev.joaolaureano.trainingkafka.orders.bootstrap.facade;

import dev.joaolaureano.trainingkafka.orders.adapters.web.FindOrderPort;
import dev.joaolaureano.trainingkafka.orders.adapters.web.OrderResponse;
import dev.joaolaureano.trainingkafka.orders.application.FindOrderUseCase;

import java.util.Objects;
import java.util.Optional;

/** Liga a consulta HTTP ao caso de uso, traduzindo a view em contrato de fio. */
public class FindOrderFacade implements FindOrderPort {

    private final FindOrderUseCase findOrder;

    public FindOrderFacade(FindOrderUseCase findOrder) {
        this.findOrder = Objects.requireNonNull(findOrder);
    }

    @Override
    public Optional<OrderResponse> byId(String orderId) {
        return findOrder.byId(orderId).map(view -> new OrderResponse(
                view.orderId(), view.status(), view.customerId(), view.product(),
                view.quantity(), view.amount(), view.placedAt().toString()));
    }
}
