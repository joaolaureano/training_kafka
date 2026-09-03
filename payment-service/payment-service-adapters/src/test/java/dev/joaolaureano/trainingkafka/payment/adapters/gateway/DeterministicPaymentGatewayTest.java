package dev.joaolaureano.trainingkafka.payment.adapters.gateway;

import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O gateway é uma simulação, e a regra é dele: um limite fixo, sem consultar o
 * fraud-service. O fraud continua paralelo e não autoriza pagamento nenhum.
 */
class DeterministicPaymentGatewayTest {

    private final PaymentGateway gateway = new DeterministicPaymentGateway(new BigDecimal("1000.00"));

    @Test
    @DisplayName("aprova no limite exato")
    void approvesUpToTheLimit() {
        assertThat(gateway.charge(payment(new BigDecimal("1000.00"))).approved()).isTrue();
    }

    @Test
    @DisplayName("recusa acima do limite, com motivo")
    void declinesAboveTheLimit() {
        PaymentGateway.GatewayResult result = gateway.charge(payment(new BigDecimal("1000.01")));

        assertThat(result.approved()).isFalse();
        assertThat(result.reason()).isNotBlank();
    }

    private static Payment payment(BigDecimal amount) {
        return Payment.request("order-1", "cust-1", amount);
    }
}
