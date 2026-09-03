package dev.joaolaureano.trainingkafka.payment.bootstrap.facade;

import dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudDetectedMessage;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudEventPort;
import dev.joaolaureano.trainingkafka.payment.application.CompensateFraudulentOrders;

/**
 * A costura entre o contrato de fio do fraud e o caso de uso de compensação.
 *
 * O motivo da compensação vira texto aqui, na borda: o domínio de Payment não
 * precisa conhecer a política de fraude, só que houve uma razão para cancelar.
 */
public final class FraudEventFacade implements FraudEventPort {

    private final CompensateFraudulentOrders compensate;

    public FraudEventFacade(CompensateFraudulentOrders compensate) {
        this.compensate = compensate;
    }

    @Override
    public void handle(FraudDetectedMessage message) {
        compensate.handle(message.customerId(),
                message.orders().stream()
                        .map(order -> new CompensateFraudulentOrders.FraudulentOrder(
                                order.orderId(), order.amount()))
                        .toList(),
                "fraud detected: " + message.ordersInWindow() + " orders in "
                        + message.windowSeconds() + "s",
                message.correlationId());
    }
}
