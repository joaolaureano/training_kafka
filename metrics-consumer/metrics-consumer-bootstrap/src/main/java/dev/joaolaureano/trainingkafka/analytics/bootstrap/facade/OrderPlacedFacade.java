package dev.joaolaureano.trainingkafka.analytics.bootstrap.facade;

import dev.joaolaureano.trainingkafka.analytics.adapters.messaging.OrderPlacedPort;
import dev.joaolaureano.trainingkafka.analytics.application.OrderPlacedHandler;
import dev.joaolaureano.trainingkafka.analytics.domain.event.OrderPlaced;

import java.util.Objects;

/** Liga o listener do tópico "orders" ao orquestrador de aplicação. */
public class OrderPlacedFacade implements OrderPlacedPort {

    private final OrderPlacedHandler handler;

    public OrderPlacedFacade(OrderPlacedHandler handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    @Override
    public void handle(OrderPlaced event) {
        handler.handle(event);
    }
}
