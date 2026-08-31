package dev.joaolaureano.trainingkafka.orders.bootstrap.facade;

import dev.joaolaureano.trainingkafka.orders.adapters.web.PlaceOrderPort;
import dev.joaolaureano.trainingkafka.orders.adapters.web.PlaceOrderRequest;
import dev.joaolaureano.trainingkafka.orders.application.PlaceOrderCommand;
import dev.joaolaureano.trainingkafka.orders.application.PlaceOrderUseCase;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;

import java.util.Objects;

/**
 * Liga o adapter web ao caso de uso.
 *
 * O controller declara o que precisa ({@link PlaceOrderPort}) e o caso de uso
 * declara o que oferece ({@link PlaceOrderUseCase}); nenhum dos dois conhece o
 * outro. Esta classe é a única no serviço que conhece os dois lados — e ela vive
 * no bootstrap, onde conhecer tudo é o trabalho.
 */
public class PlaceOrderFacade implements PlaceOrderPort {

    private final PlaceOrderUseCase placeOrder;

    public PlaceOrderFacade(PlaceOrderUseCase placeOrder) {
        this.placeOrder = Objects.requireNonNull(placeOrder);
    }

    @Override
    public OrderId place(PlaceOrderRequest request) {
        return placeOrder.handle(new PlaceOrderCommand(
                request.customerId(),
                request.product(),
                request.quantity(),
                request.amount()));
    }
}
