package dev.joaolaureano.trainingkafka.orders.bootstrap.facade;

import dev.joaolaureano.trainingkafka.orders.adapters.messaging.PaymentEventMessage;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.PaymentEventPort;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.UnknownPaymentEventException;
import dev.joaolaureano.trainingkafka.orders.application.ApplyPaymentResult;

public final class PaymentEventFacade implements PaymentEventPort {

    private final ApplyPaymentResult applyResult;

    public PaymentEventFacade(ApplyPaymentResult applyResult) {
        this.applyResult = applyResult;
    }

    @Override
    public void handle(PaymentEventMessage message) {
        if ("PaymentApproved".equals(message.eventType())) {
            applyResult.approved(message.orderId());
        } else if ("PaymentFailed".equals(message.eventType())) {
            applyResult.failed(message.orderId());
        } else if ("PaymentCancelled".equals(message.eventType())) {
            applyResult.cancelledForFraud(message.orderId());
        } else {
            throw new UnknownPaymentEventException(message.eventType());
        }
    }
}
