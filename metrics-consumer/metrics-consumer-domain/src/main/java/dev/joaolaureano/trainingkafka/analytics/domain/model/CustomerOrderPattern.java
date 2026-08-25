package dev.joaolaureano.trainingkafka.analytics.domain.model;

import dev.joaolaureano.trainingkafka.analytics.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.analytics.domain.event.SuspiciousPatternDetected;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Aggregate root: o padrão de pedidos recentes de UM cliente.
 *
 * É este agregado — e nenhum serviço externo — que decide se um comportamento é
 * suspeito. Ninguém de fora inspeciona a lista de pedidos para tirar conclusões:
 * pergunta-se ao agregado, e ele responde. A regra vive junto dos dados que ela
 * governa, que é o ponto inteiro de modelar assim.
 *
 * Invariantes:
 * <ol>
 *   <li>A janela nunca retém pedidos mais antigos que {@code policy.window} em
 *       relação ao pedido mais recente — o agregado se poda sozinho. Isso o mantém
 *       pequeno por construção, o que importa muito na hora de persistir.</li>
 *   <li>{@link #isSuspicious()} é verdadeiro se e somente se a contagem na janela
 *       atingiu {@code policy.maxOrders}.</li>
 * </ol>
 *
 * Fronteira de consistência: um cliente. Como o tópico "orders" é particionado
 * por customerId, todos os pedidos de um cliente chegam pela mesma partição e são
 * processados em série — este agregado nunca sofre escrita concorrente.
 */
public class CustomerOrderPattern {

    /** Quantos pedidos acompanham o alerta, como amostra. */
    private static final int SAMPLE_SIZE = 5;

    private final CustomerId customerId;
    private final SuspicionPolicy policy;
    private final List<OrderPlacement> recentOrders;

    private final List<DomainEvent> pendingEvents = new ArrayList<>();

    private CustomerOrderPattern(CustomerId customerId, SuspicionPolicy policy,
                                 List<OrderPlacement> recentOrders) {
        this.customerId = customerId;
        this.policy = policy;
        this.recentOrders = new ArrayList<>(recentOrders);
    }

    public static CustomerOrderPattern startFor(CustomerId customerId, SuspicionPolicy policy) {
        requireBasics(customerId, policy);
        return new CustomerOrderPattern(customerId, policy, List.of());
    }

    /**
     * Reconstrói o agregado a partir do estado persistido.
     *
     * Repare no que NÃO é reconstituído: o fato de estar sinalizado. Esse estado
     * é derivado da janela, não guardado — persistir um booleano que já é
     * calculável criaria uma segunda verdade, livre para divergir da primeira.
     */
    public static CustomerOrderPattern reconstitute(CustomerId customerId, SuspicionPolicy policy,
                                                    List<OrderPlacement> recentOrders) {
        requireBasics(customerId, policy);
        if (recentOrders == null) {
            throw new InvalidValueException("recentOrders é obrigatório (use lista vazia)");
        }
        List<OrderPlacement> ordered = new ArrayList<>(recentOrders);
        ordered.sort(java.util.Comparator.comparing(OrderPlacement::occurredAt));
        return new CustomerOrderPattern(customerId, policy, ordered);
    }

    private static void requireBasics(CustomerId customerId, SuspicionPolicy policy) {
        if (customerId == null) {
            throw new InvalidValueException("customerId é obrigatório");
        }
        if (policy == null) {
            throw new InvalidValueException("policy é obrigatória");
        }
    }

    /**
     * Registra um novo pedido do cliente e reavalia o padrão.
     *
     * O alerta dispara na TRANSIÇÃO de normal para suspeito, e não a cada pedido
     * acima do limiar. Sem isso, a rajada de 500 pedidos que o k6 vai disparar
     * produziria centenas de alertas idênticos. Quando a janela drena e a contagem
     * cai abaixo do limiar, um novo surto volta a alertar.
     */
    public void registerOrder(OrderId orderId, Instant occurredAt, Money amount) {
        OrderPlacement placement = new OrderPlacement(orderId, occurredAt, amount);

        pruneOlderThanWindowEndingAt(occurredAt);
        boolean wasSuspicious = isSuspicious();

        recentOrders.add(placement);

        if (!wasSuspicious && isSuspicious()) {
            pendingEvents.add(new SuspiciousPatternDetected(
                    customerId, recentOrders.size(), policy.window(), sampleOfRecentOrders(), occurredAt));
        }
    }

    /**
     * A decisão, em uma linha, dentro do agregado.
     *
     * A policy traz os números; a conclusão é daqui.
     */
    public boolean isSuspicious() {
        return recentOrders.size() >= policy.maxOrders();
    }

    public int ordersInWindow() {
        return recentOrders.size();
    }

    private void pruneOlderThanWindowEndingAt(Instant windowEnd) {
        Instant cutoff = windowEnd.minus(policy.window());
        recentOrders.removeIf(placement -> placement.occurredAt().isBefore(cutoff));
    }

    private List<OrderId> sampleOfRecentOrders() {
        int from = Math.max(0, recentOrders.size() - SAMPLE_SIZE);
        return recentOrders.subList(from, recentOrders.size()).stream()
                .map(OrderPlacement::orderId)
                .toList();
    }

    /** Devolve os fatos acumulados e esvazia a lista. */
    public List<DomainEvent> pullDomainEvents() {
        List<DomainEvent> drained = List.copyOf(pendingEvents);
        pendingEvents.clear();
        return drained;
    }

    public List<DomainEvent> pendingEvents() {
        return Collections.unmodifiableList(pendingEvents);
    }

    public CustomerId customerId() {
        return customerId;
    }

    public SuspicionPolicy policy() {
        return policy;
    }

    /** Exposto para os repositórios persistirem a janela. Cópia defensiva, imutável. */
    public List<OrderPlacement> recentOrders() {
        return List.copyOf(recentOrders);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof CustomerOrderPattern other && customerId.equals(other.customerId);
    }

    @Override
    public int hashCode() {
        return customerId.hashCode();
    }

    @Override
    public String toString() {
        return "CustomerOrderPattern[" + customerId + " ordersInWindow=" + recentOrders.size()
                + " suspicious=" + isSuspicious() + "]";
    }
}
