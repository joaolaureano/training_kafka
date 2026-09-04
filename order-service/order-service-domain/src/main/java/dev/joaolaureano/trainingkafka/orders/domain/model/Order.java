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
    private OrderStatus status;

    private final List<DomainEvent> pendingEvents = new ArrayList<>();

    private Order(OrderId id, CustomerId customerId, ProductId productId,
                  Quantity quantity, Money amount, Instant placedAt, OrderStatus status) {
        this.id = id;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.placedAt = placedAt;
        this.status = status;
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
            orderedQuantity, orderAmount, placedAt, OrderStatus.PENDING_STOCK);
        order.pendingEvents.add(new OrderPlaced(
                order.id, customer, productId, orderedQuantity, orderAmount, placedAt));
        return order;
    }

    public static Order reconstitute(OrderId id, CustomerId customerId, ProductId productId,
                                     Quantity quantity, Money amount, Instant placedAt,
                                     OrderStatus status) {
        if (id == null || status == null) {
            throw new InvalidOrderException("order state is incomplete");
        }
        return new Order(id, customerId, productId, quantity, amount, placedAt, status);
    }

    /**
     * O estoque foi separado: o pedido pode seguir para a cobrança.
     *
     * É a primeira transição da Saga, e ela existe para que exista um estado em
     * que o pedido foi aceito mas ainda não pode ser cobrado. Sem ele, cobrar e
     * reservar seriam simultâneos — e o sistema poderia cobrar por algo que não
     * tem para entregar.
     *
     * <p>Só avança a partir de PENDING_STOCK; em qualquer outro estado é no-op, e
     * isso NÃO é permissividade. Este pedido recebe fatos de dois tópicos
     * diferentes — {@code inventory-events} e {@code payment-events} —, e o Kafka
     * não ordena um tópico em relação ao outro. Um StockReserved que chega depois
     * de o pagamento já ter sido aplicado é a mesma verdade chegando atrasada, não
     * uma contradição: não há nada a fazer com ele.
     */
    public void confirmStock() {
        if (status != OrderStatus.PENDING_STOCK) {
            return;
        }
        status = OrderStatus.PENDING_PAYMENT;
    }

    /**
     * Não havia estoque — ou o produto sequer existe no catálogo.
     *
     * Separada de {@link #cancelForPaymentFailure()} pelo mesmo motivo que a
     * compensação por fraude é: são causas diferentes, e um método só, permissivo,
     * apagaria a diferença junto com a guarda. É o único cancelamento que acontece
     * sem que o dinheiro tenha se movido.
     *
     * <p>A guarda continua estrita, e aqui ela é legítima: uma reserva é decidida
     * uma única vez, então StockRejected e StockReserved são mutuamente exclusivos
     * para o mesmo pedido. Recusar estoque a um pedido que já foi cobrado não é
     * atraso de tópico — é impossível.
     */
    public void cancelForOutOfStock() {
        if (status == OrderStatus.CANCELLED) {
            return;
        }
        if (status != OrderStatus.PENDING_STOCK) {
            throw new InvalidOrderTransitionException(id, status, OrderStatus.CANCELLED);
        }
        status = OrderStatus.CANCELLED;
    }

    /**
     * O pagamento foi aprovado.
     *
     * Reentregar o mesmo PaymentApproved é normal — o consumidor é at-least-once —
     * e o segundo apply não faz nada. Já aprovar um pedido CANCELLED não é
     * duplicata: é contradição, e recusar é o que impede a Saga de terminar num
     * estado que nenhuma sequência de eventos legítima produziria.
     *
     * <p><b>PENDING_STOCK também é aceito, e a razão é sutil.</b> Parece a
     * contradição óbvia — cobrar sem estoque —, e foi assim que esta guarda nasceu.
     * Mas o payment-service só é disparado por {@code StockReserved}: se existe um
     * resultado de pagamento, o estoque FOI reservado. Encontrar o pedido ainda em
     * PENDING_STOCK significa apenas que o StockReserved correspondente ainda não
     * foi consumido por este serviço — os dois fatos vêm de tópicos diferentes, e
     * o Kafka não ordena um tópico em relação ao outro.
     *
     * <p>Recusar aqui não protegia invariante nenhuma: mandava um resultado de
     * pagamento legítimo para a DLQ e deixava o pedido preso em PENDING_PAYMENT
     * para sempre, quando o StockReserved chegasse depois. Quem garante que não se
     * cobra sem estoque é estrutural, e está do outro lado: o payment-service não
     * escuta {@code orders}.
     */
    public void approvePayment() {
        if (status == OrderStatus.PAID) {
            return;
        }
        if (status != OrderStatus.PENDING_STOCK && status != OrderStatus.PENDING_PAYMENT) {
            throw new InvalidOrderTransitionException(id, status, OrderStatus.PAID);
        }
        status = OrderStatus.PAID;
    }

    /**
     * A compensação por fraude, que chega DEPOIS de o pedido já estar pago.
     *
     * É a única transição que sai de PAID, e existe separada de
     * {@link #cancelForPaymentFailure()} de propósito: cancelar um pedido pago
     * porque o pagamento falhou é contradição, cancelá-lo porque a fraude foi
     * detectada depois é fluxo legítimo. Um método só, permissivo, apagaria a
     * diferença — e com ela a guarda que protege a Saga.
     */
    public void cancelForFraud() {
        if (status == OrderStatus.CANCELLED) {
            return;
        }
        status = OrderStatus.CANCELLED;
    }

    /**
     * A compensação da Saga: o pagamento falhou, o pedido não se sustenta.
     *
     * Aceita PENDING_STOCK pelo mesmo motivo que {@link #approvePayment()}: um
     * PaymentFailed só existe se houve StockReserved antes. Continua recusando a
     * partir de PAID, que é a contradição de verdade — o mesmo pagamento não pode
     * ter sido aprovado e ter falhado.
     */
    public void cancelForPaymentFailure() {
        if (status == OrderStatus.CANCELLED) {
            return;
        }
        if (status != OrderStatus.PENDING_STOCK && status != OrderStatus.PENDING_PAYMENT) {
            throw new InvalidOrderTransitionException(id, status, OrderStatus.CANCELLED);
        }
        status = OrderStatus.CANCELLED;
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

    public OrderStatus status() {
        return status;
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
