package dev.joaolaureano.trainingkafka.payment.application;

import dev.joaolaureano.trainingkafka.payment.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.payment.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.model.PaymentStatus;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
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
    private final ActivityLogPublisher activityLog;
    private final Clock clock;

    public CompensateFraudulentOrders(PaymentRepository payments, ActivityLogPublisher activityLog,
                                      Clock clock) {
        this.payments = Objects.requireNonNull(payments);
        this.activityLog = Objects.requireNonNull(activityLog);
        this.clock = Objects.requireNonNull(clock);
    }

    public void handle(String customerId, List<FraudulentOrder> orders, String reason,
                       String correlationId) {
        Instant now = clock.instant();

        for (FraudulentOrder fraudulent : orders) {
            Payment payment = payments.findByOrderId(fraudulent.orderId())
                    .orElseGet(() -> Payment.request(fraudulent.orderId(), customerId,
                            fraudulent.amount()));

            PaymentStatus before = payment.status();
            payment.cancelForFraud(now, reason, correlationId);

            if (payment.status() == before) {
                // Já cancelado antes, ou falhado: não há dinheiro a devolver. Vale
                // registrar mesmo assim — para quem audita, "por que este pedido
                // fraudulento não foi estornado?" é uma pergunta legítima, e o
                // silêncio não a responde.
                audit("payment.compensation.skipped", ActivityLog::info, payment, before, reason,
                        correlationId, now);
                continue;
            }

            // Estorno e aviso de estorno no mesmo commit: compensar sem que ninguém
            // saiba é tão ruim quanto não compensar.
            payments.save(payment, payment.pullDomainEvents());

            audit(payment.refunded() ? "payment.refunded" : "payment.cancelled",
                    ActivityLog::warn, payment, before, reason, correlationId, now);
        }
    }

    private void audit(String action, LogFactory level, Payment payment, PaymentStatus before,
                       String reason, String correlationId, Instant at) {
        activityLog.publish(level.create(action, ActivityLog.context(
                "orderId", payment.orderId(),
                "paymentId", payment.id().toString(),
                "amount", payment.amount().toPlainString(),
                // O estado anterior é o que distingue "devolvemos dinheiro" de
                // "cancelamos antes de cobrar" na hora de conciliar.
                "previousStatus", before.name(),
                "refunded", Boolean.toString(payment.refunded()),
                "reason", String.valueOf(reason),
                "correlationId", String.valueOf(correlationId)), at));
    }

    @FunctionalInterface
    private interface LogFactory {
        ActivityLog create(String action, java.util.Map<String, String> context, Instant at);
    }

    /** Um pedido da janela suspeita: o identificador e quanto há para estornar. */
    public record FraudulentOrder(String orderId, BigDecimal amount) {
    }
}
