package dev.joaolaureano.trainingkafka.payment.domain.model;

import dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentApproved;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentCancelled;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentFailed;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate root do contexto de Payment.
 *
 * Guarda o desfecho — quando aconteceu, por quê, sob qual correlação — e não só o
 * estado. Um status sozinho não diz por que o pagamento foi recusado nem sob qual
 * correlação, e é isso que alguém investigando um estorno precisa ler.
 */
public final class Payment {

    private final PaymentId id;
    private final String orderId;
    private final String customerId;
    private final BigDecimal amount;
    private PaymentStatus status;
    private Instant resolvedAt;
    private String reason;
    private String correlationId;
    private boolean refunded;

    private final List<DomainEvent> pendingEvents = new ArrayList<>();

    private Payment(PaymentId id, String orderId, String customerId, BigDecimal amount,
                    PaymentStatus status, Instant resolvedAt, String reason, String correlationId,
                    boolean refunded) {
        this.id = id;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.resolvedAt = resolvedAt;
        this.reason = reason;
        this.correlationId = correlationId;
        this.refunded = refunded;
    }

    public static Payment request(String orderId, String customerId, BigDecimal amount) {
        requireText(orderId, "orderId");
        requireText(customerId, "customerId");
        requireAmount(amount);
        return new Payment(PaymentId.generate(), orderId, customerId, amount,
                PaymentStatus.PENDING, null, null, null, false);
    }

    public static Payment reconstitute(PaymentId id, String orderId, String customerId,
                                       BigDecimal amount, PaymentStatus status,
                                       Instant resolvedAt, String reason, String correlationId,
                                       boolean refunded) {
        if (id == null || status == null) {
            throw new IllegalArgumentException("payment state is incomplete");
        }
        requireText(orderId, "orderId");
        requireText(customerId, "customerId");
        requireAmount(amount);
        return new Payment(id, orderId, customerId, amount, status, resolvedAt, reason,
                correlationId, refunded);
    }

    public void approve(Instant occurredAt, String correlationId) {
        if (status != PaymentStatus.PENDING) {
            return;
        }
        status = PaymentStatus.APPROVED;
        resolvedAt = occurredAt;
        this.correlationId = correlationId;
        pendingEvents.add(approvedEvent());
    }

    public void fail(Instant occurredAt, String reason, String correlationId) {
        if (status != PaymentStatus.PENDING) {
            return;
        }
        status = PaymentStatus.FAILED;
        resolvedAt = occurredAt;
        this.reason = reason;
        this.correlationId = correlationId;
        pendingEvents.add(failedEvent());
    }

    /**
     * Compensa: a fraude foi detectada depois, e o que já foi cobrado precisa voltar.
     *
     * Não é rollback — nada é desfeito. É um movimento novo, no sentido inverso, e
     * por isso emite um fato próprio em vez de apagar o anterior.
     *
     * Um pagamento que já FALHOU não tem o que compensar: o dinheiro nunca saiu e o
     * pedido já foi cancelado pela própria falha. Reaplicar sobre CANCELLED é no-op,
     * porque a reentrega do mesmo FraudDetected é esperada.
     */
    public void cancelForFraud(Instant occurredAt, String reason, String correlationId) {
        if (status == PaymentStatus.CANCELLED || status == PaymentStatus.FAILED) {
            return;
        }
        refunded = status == PaymentStatus.APPROVED;
        status = PaymentStatus.CANCELLED;
        resolvedAt = occurredAt;
        this.reason = reason;
        this.correlationId = correlationId;
        pendingEvents.add(cancelledEvent());
    }

    private PaymentApproved approvedEvent() {
        return new PaymentApproved(id.toString(), orderId, customerId, amount, resolvedAt, correlationId);
    }

    private PaymentCancelled cancelledEvent() {
        return new PaymentCancelled(id.toString(), orderId, customerId, amount, resolvedAt,
                reason, correlationId, refunded);
    }

    private PaymentFailed failedEvent() {
        return new PaymentFailed(id.toString(), orderId, customerId, amount, resolvedAt, reason, correlationId);
    }

    public List<DomainEvent> pullDomainEvents() {
        List<DomainEvent> events = List.copyOf(pendingEvents);
        pendingEvents.clear();
        return events;
    }

    public PaymentId id() { return id; }
    public String orderId() { return orderId; }
    public String customerId() { return customerId; }
    public BigDecimal amount() { return amount; }
    public PaymentStatus status() { return status; }
    public Instant resolvedAt() { return resolvedAt; }
    public String reason() { return reason; }
    public String correlationId() { return correlationId; }
    public boolean refunded() { return refunded; }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }

    private static void requireAmount(BigDecimal amount) {
        if (amount == null || amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
    }
}
