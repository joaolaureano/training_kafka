package dev.joaolaureano.trainingkafka.inventory.domain.model;

import dev.joaolaureano.trainingkafka.inventory.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockRejected;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockReleased;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockReserved;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Aggregate root da participação deste contexto na Saga: o que foi decidido para
 * um pedido, e uma vez só.
 *
 * <p>A identidade é o {@code orderId}, e isso é a idempotência inteira. O consumo
 * é at-least-once: o mesmo OrderPlaced chega duas vezes com frequência normal, e
 * sem uma identidade estável a segunda entrega descontaria o estoque de novo. Com
 * ela, a segunda entrega encontra a reserva que já existe e não faz nada.
 *
 * <p>{@link Product} guarda quanto existe; esta classe guarda para quem foi
 * separado. São dois agregados porque têm ciclos de vida diferentes — o produto
 * sobrevive a todos os pedidos —, mas as duas gravações acontecem na mesma
 * transação, senão a Saga poderia ver estoque descontado sem reserva registrada.
 */
public class Reservation {

    private final String orderId;
    private final Sku sku;
    private final Quantity quantity;
    private final String customerId;
    private final BigDecimal amount;
    private final String correlationId;
    private ReservationStatus status;
    private Instant decidedAt;
    private String reason;

    private final List<DomainEvent> pendingEvents = new ArrayList<>();

    private Reservation(String orderId, Sku sku, Quantity quantity, String customerId,
                        BigDecimal amount, String correlationId, ReservationStatus status,
                        Instant decidedAt, String reason) {
        this.orderId = orderId;
        this.sku = sku;
        this.quantity = quantity;
        this.customerId = customerId;
        this.amount = amount;
        this.correlationId = correlationId;
        this.status = status;
        this.decidedAt = decidedAt;
        this.reason = reason;
    }

    /** As unidades foram separadas. */
    public static Reservation held(String orderId, Sku sku, Quantity quantity, String customerId,
                                   BigDecimal amount, String correlationId, Instant at) {
        Reservation reservation = new Reservation(requireOrderId(orderId), sku, quantity, customerId,
                amount, correlationId, ReservationStatus.HELD, at, null);
        reservation.pendingEvents.add(new StockReserved(
                reservation.orderId, sku, quantity, customerId, amount, correlationId, at));
        return reservation;
    }

    /** Não havia o que separar — e o pedido morre aqui, antes de qualquer cobrança. */
    public static Reservation rejected(String orderId, Sku sku, Quantity quantity, String customerId,
                                       BigDecimal amount, String correlationId,
                                       RejectionReason reason, Instant at) {
        Reservation reservation = new Reservation(requireOrderId(orderId), sku, quantity, customerId,
                amount, correlationId, ReservationStatus.REJECTED, at, reason.name());
        reservation.pendingEvents.add(new StockRejected(
                reservation.orderId, sku, quantity, customerId, amount, correlationId, reason, at));
        return reservation;
    }

    /**
     * A lápide: o pedido acabou antes de haver o que reservar.
     *
     * Não carrega SKU nem quantidade porque não os conhece — quem a cria é o
     * desfecho do pagamento, que fala de dinheiro, não de estoque. E não levanta
     * evento nenhum: nada se moveu, e o pedido já foi cancelado por quem mandou
     * esta mensagem.
     *
     * O valor dela é ser encontrada depois. A guarda de idempotência da reserva
     * consulta por {@code orderId} antes de decidir qualquer coisa, então o
     * OrderPlaced que chegar atrasado esbarra nesta linha e não reserva nada.
     */
    public static Reservation voided(String orderId, String customerId, BigDecimal amount,
                                     String correlationId, String reason, Instant at) {
        return new Reservation(requireOrderId(orderId), null, Quantity.ZERO, customerId,
                amount, correlationId, ReservationStatus.VOIDED, at, reason);
    }

    public static Reservation reconstitute(String orderId, Sku sku, Quantity quantity,
                                           String customerId, BigDecimal amount, String correlationId,
                                           ReservationStatus status, Instant decidedAt, String reason) {
        if (orderId == null || status == null) {
            throw new InvalidProductException("reservation state is incomplete");
        }
        // sku nulo só é legítimo na lápide — ver Reservation.voided.
        if (sku == null && status != ReservationStatus.VOIDED) {
            throw new InvalidProductException("reservation without sku: " + orderId);
        }
        return new Reservation(orderId, sku, quantity, customerId, amount, correlationId,
                status, decidedAt, reason);
    }

    /**
     * Devolve as unidades.
     *
     * Só faz sentido sobre uma reserva que de fato segura estoque — daí a guarda em
     * {@link #isHeld()} do lado de quem chama. Reentregar o mesmo PaymentFailed é
     * normal, e a segunda passagem não pode devolver estoque outra vez: seria criar
     * unidades do nada, que é o mesmo erro do oversell com o sinal trocado.
     */
    public void release(String reason, Instant at) {
        if (status != ReservationStatus.HELD) {
            return;
        }
        status = ReservationStatus.RELEASED;
        this.reason = reason;
        this.decidedAt = at;
        pendingEvents.add(new StockReleased(orderId, sku, quantity, customerId, amount,
                correlationId, reason, at));
    }

    public boolean isHeld() {
        return status == ReservationStatus.HELD;
    }

    /** Devolve os fatos acumulados e esvazia a lista — chamado uma única vez, pela aplicação. */
    public List<DomainEvent> pullDomainEvents() {
        List<DomainEvent> drained = List.copyOf(pendingEvents);
        pendingEvents.clear();
        return drained;
    }

    public List<DomainEvent> pendingEvents() {
        return Collections.unmodifiableList(pendingEvents);
    }

    private static String requireOrderId(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new InvalidProductException("orderId é obrigatório numa reserva");
        }
        return orderId;
    }

    public String orderId() {
        return orderId;
    }

    public Sku sku() {
        return sku;
    }

    public Quantity quantity() {
        return quantity;
    }

    public String customerId() {
        return customerId;
    }

    public BigDecimal amount() {
        return amount;
    }

    public String correlationId() {
        return correlationId;
    }

    public ReservationStatus status() {
        return status;
    }

    public Instant decidedAt() {
        return decidedAt;
    }

    public String reason() {
        return reason;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Reservation reservation && orderId.equals(reservation.orderId);
    }

    @Override
    public int hashCode() {
        return orderId.hashCode();
    }

    @Override
    public String toString() {
        return "Reservation[" + orderId + " sku=" + sku + " qty=" + quantity + " " + status + "]";
    }
}
