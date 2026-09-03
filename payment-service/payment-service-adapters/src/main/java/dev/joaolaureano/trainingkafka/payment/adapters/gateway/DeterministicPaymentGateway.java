package dev.joaolaureano.trainingkafka.payment.adapters.gateway;

import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway;

import java.math.BigDecimal;

public final class DeterministicPaymentGateway implements PaymentGateway {

    private final BigDecimal approvalLimit;

    public DeterministicPaymentGateway(BigDecimal approvalLimit) {
        this.approvalLimit = approvalLimit;
    }

    @Override
    public GatewayResult charge(Payment payment) {
        if (payment.amount().compareTo(approvalLimit) <= 0) {
            return GatewayResult.accepted();
        }
        return GatewayResult.declined("amount exceeds fake gateway approval limit");
    }
}
