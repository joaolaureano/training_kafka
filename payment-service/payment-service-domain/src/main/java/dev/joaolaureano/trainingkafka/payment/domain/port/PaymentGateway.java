package dev.joaolaureano.trainingkafka.payment.domain.port;

import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;

public interface PaymentGateway {
    GatewayResult charge(Payment payment);

    record GatewayResult(boolean approved, String reason) {
        public static GatewayResult accepted() { return new GatewayResult(true, null); }
        public static GatewayResult declined(String reason) { return new GatewayResult(false, reason); }
    }
}
