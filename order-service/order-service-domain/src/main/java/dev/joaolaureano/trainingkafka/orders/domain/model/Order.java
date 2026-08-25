package dev.joaolaureano.trainingkafka.orders.domain.model;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.event.OrderPlaced;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Aggregate root do contexto de Ordering.
 *
 * Só existe uma forma de criar um pedido — {@link #place} — e ela é a única
 * guardiã das invariantes. Não há construtor público, não há setter: um Order
 * que exista é, por construção, um Order válido.
 */
public class Order {

    private final OrderId id;
    private final CustomerId customerId;
    private final ProductId productId;
    private final Quantity quantity;
    private final Money amount;
    private final Instant placedAt;

    private final List<DomainEvent> pendingEvents = new ArrayList<>();

    private Order(OrderId id, CustomerId customerId, ProductId productId,
                  Quantity quantity, Money amount, Instant placedAt) {
        this.id = id;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.placedAt = placedAt;
    }

    /**
     * Registra um novo pedido a partir de valores crus, vindos de fora.
     *
     * Esta é a ÚNICA porta de entrada para criar um Order, e portanto o único
     * lugar do sistema onde a validação de um pedido acontece. Não existe uma
     * segunda cópia da regra num DTO de API: se {@code quantity} precisa ser
     * positiva, isso está escrito aqui e em nenhum outro lugar.
     *
     * As violações são acumuladas em vez de interrompidas na primeira, para que
     * quem enviou o pedido receba de uma vez a lista completa do que corrigir.
     */
    public static Order place(String customerId, String product, Integer quantity,
                              BigDecimal amount, Instant placedAt) {
        Violations violations = new Violations();

        CustomerId customer = violations.capture("customerId", () -> new CustomerId(customerId));
        ProductId productId = violations.capture("product", () -> new ProductId(product));
        Quantity orderedQuantity = violations.capture("quantity", () -> {
            if (quantity == null) {
                throw new InvalidOrderException("quantity é obrigatório");
            }
            return new Quantity(quantity);
        });
        Money orderAmount = violations.capture("amount", () -> new Money(amount));

        // Invariante do agregado, não do value object: Money aceita zero — é um
        // valor monetário legítimo — mas um PEDIDO de valor zero não é.
        if (orderAmount != null && !orderAmount.isPositive()) {
            violations.add("amount", "amount de um pedido deve ser maior que zero");
        }
        if (placedAt == null) {
            violations.add("placedAt", "placedAt é obrigatório");
        }

        violations.throwIfAny();

        Order order = new Order(OrderId.generate(), customer, productId,
                orderedQuantity, orderAmount, placedAt);
        order.pendingEvents.add(new OrderPlaced(
                order.id, customer, productId, orderedQuantity, orderAmount, placedAt));
        return order;
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

    public OrderId id() {
        return id;
    }

    public CustomerId customerId() {
        return customerId;
    }

    public ProductId productId() {
        return productId;
    }

    public Quantity quantity() {
        return quantity;
    }

    public Money amount() {
        return amount;
    }

    public Instant placedAt() {
        return placedAt;
    }

    /** Agregados têm identidade: dois Order são o mesmo se têm o mesmo id. */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof Order other && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Order[" + id + " customer=" + customerId + " product=" + productId
                + " qty=" + quantity + " amount=" + amount + "]";
    }
}
