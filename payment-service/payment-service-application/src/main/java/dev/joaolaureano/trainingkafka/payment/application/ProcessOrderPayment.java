package dev.joaolaureano.trainingkafka.payment.application;

import dev.joaolaureano.trainingkafka.payment.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.payment.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.model.PaymentStatus;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Caso de uso: cobrar o pedido que acabou de ser aceito.
 *
 * A idempotência mora aqui, e é o que torna o consumo at-least-once seguro: o
 * pagamento é procurado por orderId antes de qualquer coisa, e um pagamento que
 * já saiu de PENDING não é cobrado de novo. Reentregar OrderPlaced não gera
 * segunda cobrança nem segundo evento.
 *
 * Nada é publicado em payment-events aqui. O desfecho e o evento vão para o banco
 * no mesmo commit, e o relay do outbox entrega depois — é o que garante que um
 * pagamento resolvido nunca fique com o resultado preso dentro do processo.
 */
public final class ProcessOrderPayment {

    private final PaymentRepository payments;
    private final PaymentGateway gateway;
    private final ActivityLogPublisher activityLog;
    private final Clock clock;

    public ProcessOrderPayment(PaymentRepository payments, PaymentGateway gateway,
                               ActivityLogPublisher activityLog, Clock clock) {
        this.payments = Objects.requireNonNull(payments);
        this.gateway = Objects.requireNonNull(gateway);
        this.activityLog = Objects.requireNonNull(activityLog);
        this.clock = Objects.requireNonNull(clock);
    }

    public void handle(String orderId, String customerId, BigDecimal amount, String correlationId) {
        Payment payment = payments.findByOrderId(orderId)
                .orElseGet(() -> Payment.request(orderId, customerId, amount));

        if (payment.status() != PaymentStatus.PENDING) {
            // Já resolvido. Não republica: se o pagamento está gravado, o evento dele
            // está na mesma transação — ou já saiu, ou o relay ainda vai entregar.
            return;
        }

        Instant now = clock.instant();
        String correlation = correlationId == null ? UUID.randomUUID().toString() : correlationId;
        PaymentGateway.GatewayResult result = gateway.charge(payment);

        if (result.approved()) {
            payment.approve(now, correlation);
        } else {
            payment.fail(now, result.reason(), correlation);
        }

        payments.save(payment, payment.pullDomainEvents());
        audit(payment, result, now, correlation);
    }

    /**
     * Recusa do gateway é WARN, não ERROR: o pagamento não deu certo, mas o
     * sistema funcionou exatamente como devia. ERROR seria para quando o
     * mecanismo em si falha, e aí a exceção sobe e a mensagem vai para a DLQ —
     * sem passar por aqui.
     */
    private void audit(Payment payment, PaymentGateway.GatewayResult result, Instant at,
                       String correlationId) {
        if (result.approved()) {
            activityLog.publish(ActivityLog.info("payment.approved", ActivityLog.context(
                    "orderId", payment.orderId(),
                    "paymentId", payment.id().toString(),
                    "amount", payment.amount().toPlainString(),
                    "correlationId", correlationId), at));
        } else {
            activityLog.publish(ActivityLog.warn("payment.declined", ActivityLog.context(
                    "orderId", payment.orderId(),
                    "paymentId", payment.id().toString(),
                    "amount", payment.amount().toPlainString(),
                    "reason", String.valueOf(result.reason()),
                    "correlationId", correlationId), at));
        }
    }
}
