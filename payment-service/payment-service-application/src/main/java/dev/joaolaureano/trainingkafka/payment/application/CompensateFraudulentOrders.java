package dev.joaolaureano.trainingkafka.payment.application;

import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.Objects;

/**
 * Caso de uso: a fraude foi detectada depois do fato, e a rajada precisa ser
 * compensada.
 *
 * Um FraudDetected vira N compensações — a janela inteira, um pagamento por
 * pedido. Cada uma é independente e idempotente, então reentregar o evento não
 * estorna duas vezes.
 *
 * O pagamento pode ainda não existir: fraud e payment consomem {@code orders} em
 * grupos separados, e nada garante quem chega primeiro. Nesse caso o pagamento é
 * criado JÁ cancelado — o que também fecha a porta para o OrderPlaced que ainda
 * está a caminho, porque {@link ProcessOrderPayment} não cobra um pagamento que
 * não está PENDING.
 */
public final class CompensateFraudulentOrders {

    private final PaymentRepository payments;
    private final Clock clock;

    public CompensateFraudulentOrders(PaymentRepository payments, Clock clock) {
        this.payments = Objects.requireNonNull(payments);
        this.clock = Objects.requireNonNull(clock);
    }

    public void handle(String customerId, List<FraudulentOrder> orders, String reason,
                       String correlationId) {
        for (FraudulentOrder fraudulent : orders) {
            Payment payment = payments.findByOrderId(fraudulent.orderId())
                    .orElseGet(() -> Payment.request(fraudulent.orderId(), customerId,
                            fraudulent.amount()));

            payment.cancelForFraud(clock.instant(), reason, correlationId);
            // Estorno e aviso de estorno no mesmo commit: compensar sem que ninguém
            // saiba é tão ruim quanto não compensar.
            payments.save(payment, payment.pullDomainEvents());
        }
    }

    /** Um pedido da janela suspeita: o identificador e quanto há para estornar. */
    public record FraudulentOrder(String orderId, BigDecimal amount) {
    }
}
