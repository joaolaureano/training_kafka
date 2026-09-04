package dev.joaolaureano.trainingkafka.orders.domain.port;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * O estado do pedido e os fatos que ele ainda deve ao mundo.
 *
 * As duas coisas cabem no mesmo Port de propósito: {@link #save(Order, List)} é a
 * promessa de que gravar o pedido e registrar seus eventos acontece numa
 * transação só. É essa promessa que fecha a janela entre "o pedido existe" e "o
 * mundo soube dele" — a publicação em si vira responsabilidade de quem drena o
 * registro depois (o outbox), não do caso de uso.
 */
public interface OrderRepository {

    Optional<Order> findById(OrderId orderId);

    /** Grava o pedido e enfileira seus eventos atomicamente. */
    void save(Order order, List<DomainEvent> events);

    /** Grava só a mudança de estado — usado quando a transição não gera evento novo. */
    void save(Order order);

    /**
     * Carrega, aplica a transição e grava — atomicamente.
     *
     * Existe porque o pedido passou a ter DOIS escritores concorrentes: os
     * resultados de estoque e de pagamento chegam por tópicos diferentes, em
     * threads diferentes, e escrevem no mesmo agregado. Um {@code findById}
     * seguido de {@code save} deixa a janela clássica do lost update — os dois
     * leem PENDING_STOCK, um grava PAID, o outro grava PENDING_PAYMENT por cima, e
     * o pedido fica preso num estado que nenhum dos dois eventos pediu. Sem
     * exceção, sem DLQ, sem sinal.
     *
     * Diferente do estoque, aqui não é preciso REDECIDIR sob conflito: a transição
     * é incondicional, então serializar a leitura-escrita basta. Quem precisa
     * decidir de novo — porque a resposta pode mudar — é o inventory-service, e é
     * por isso que lá a solução é bloqueio otimista com releitura, e aqui não.
     *
     * @return {@code false} se o pedido não existe. O que isso significa é decisão
     *         do caso de uso, não do repositório — daí um booleano em vez de uma
     *         exceção escolhida aqui.
     * @throws dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderTransitionException
     *         se a transição for contraditória
     */
    boolean applyTransition(OrderId orderId, Consumer<Order> transition);
}
