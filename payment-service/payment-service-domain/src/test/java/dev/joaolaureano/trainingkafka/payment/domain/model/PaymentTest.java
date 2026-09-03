package dev.joaolaureano.trainingkafka.payment.domain.model;

import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentApproved;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentCancelled;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentFailed;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    @Test
    void startsPendingAndApprovesOnce() {
        Payment payment = Payment.request("order-1", "cust-1", BigDecimal.TEN);

        payment.approve(NOW, "corr-1");
        payment.approve(NOW, "corr-1");

        assertThat(payment.status()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(payment.pullDomainEvents()).hasSize(1).first().isInstanceOf(PaymentApproved.class);
    }

    @Test
    void reconstitutedPaymentKeepsItsOutcome() {
        Payment loaded = Payment.reconstitute(PaymentId.generate(), "order-1", "cust-1",
                BigDecimal.TEN, PaymentStatus.FAILED, NOW, "declined", "corr-1", false);

        assertThat(loaded.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(loaded.reason()).isEqualTo("declined");
        assertThat(loaded.correlationId()).isEqualTo("corr-1");
        assertThat(loaded.resolvedAt()).isEqualTo(NOW);
        // Reidratar não reabre fatos já publicados.
        assertThat(loaded.pullDomainEvents()).isEmpty();
    }

    @Test
    void fraudRefundsAnApprovedPayment() {
        Payment payment = Payment.request("order-1", "cust-1", BigDecimal.TEN);
        payment.approve(NOW, "corr-1");
        payment.pullDomainEvents();

        payment.cancelForFraud(NOW, "fraud", "corr-2");

        assertThat(payment.status()).isEqualTo(PaymentStatus.CANCELLED);
        assertThat(payment.refunded()).isTrue();
        assertThat(payment.pullDomainEvents()).singleElement()
                .isInstanceOfSatisfying(PaymentCancelled.class, event -> {
                    assertThat(event.refunded()).isTrue();
                    assertThat(event.reason()).isEqualTo("fraud");
                });
    }

    @Test
    void fraudOnAPendingPaymentCancelsWithoutRefund() {
        Payment payment = Payment.request("order-1", "cust-1", BigDecimal.TEN);

        payment.cancelForFraud(NOW, "fraud", "corr-2");

        assertThat(payment.status()).isEqualTo(PaymentStatus.CANCELLED);
        // Nada foi cobrado, então não há o que estornar — e o evento não mente sobre isso.
        assertThat(payment.refunded()).isFalse();
        assertThat(payment.pullDomainEvents()).singleElement()
                .isInstanceOfSatisfying(PaymentCancelled.class, event ->
                        assertThat(event.refunded()).isFalse());
    }

    @Test
    void fraudOnAFailedPaymentIsANoOp() {
        Payment payment = Payment.request("order-1", "cust-1", BigDecimal.TEN);
        payment.fail(NOW, "declined", "corr-1");
        payment.pullDomainEvents();

        payment.cancelForFraud(NOW, "fraud", "corr-2");

        // O dinheiro nunca saiu e o pedido já foi cancelado pela própria falha.
        assertThat(payment.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.pullDomainEvents()).isEmpty();
    }

    @Test
    void fraudReappliedIsIdempotent() {
        Payment payment = Payment.request("order-1", "cust-1", BigDecimal.TEN);
        payment.approve(NOW, "corr-1");
        payment.pullDomainEvents();

        payment.cancelForFraud(NOW, "fraud", "corr-2");
        payment.pullDomainEvents();
        payment.cancelForFraud(NOW, "fraud", "corr-2");

        assertThat(payment.pullDomainEvents()).isEmpty();
    }

    @Test
    void failureIsTerminalAndDoesNotAllowApproval() {
        Payment payment = Payment.request("order-1", "cust-1", BigDecimal.TEN);

        payment.fail(NOW, "declined", "corr-1");
        payment.approve(NOW, "corr-1");

        assertThat(payment.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.pullDomainEvents()).hasSize(1).first().isInstanceOf(PaymentFailed.class);
    }
}
