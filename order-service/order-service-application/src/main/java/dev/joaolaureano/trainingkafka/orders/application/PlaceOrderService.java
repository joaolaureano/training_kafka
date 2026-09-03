package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderException;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.orders.domain.model.Violation;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Caso de uso: registrar um pedido.
 *
 * Zero anotações — este módulo não tem Spring no classpath e não conseguiria
 * escrever {@code @Service} nem se quisesse. O wiring acontece no módulo
 * -bootstrap.
 *
 * Note também que não há nenhuma regra de negócio aqui: nenhum {@code if} decide
 * se o pedido é válido. Quem decide é o agregado. Esta classe só traduz, coordena
 * e registra.
 *
 * O caso de uso NÃO publica em Kafka. Ele grava o pedido e seus eventos na mesma
 * transação, e é o outbox que entrega depois. Publicar aqui reabriria a janela
 * que a Saga não tolera: processo morto entre o insert e o send deixaria um
 * pedido PENDING_PAYMENT que ninguém jamais pagaria.
 */
public class PlaceOrderService implements PlaceOrderUseCase {

    private final OrderRepository orders;
    private final ActivityLogPublisher activityLog;
    private final Clock clock;

    public PlaceOrderService(OrderRepository orders,
                             ActivityLogPublisher activityLog,
                             Clock clock) {
        this.orders = Objects.requireNonNull(orders);
        this.activityLog = Objects.requireNonNull(activityLog);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public OrderId handle(PlaceOrderCommand command) {
        Instant now = clock.instant();
        try {
            Order order = Order.place(
                    command.customerId(),
                    command.product(),
                    command.quantity(),
                    command.amount(),
                    now);

            orders.save(order, order.pullDomainEvents());

            activityLog.publish(ActivityLog.info("order.accepted", ActivityLog.context(
                    "orderId", order.id().toString(),
                    "product", order.productId().toString(),
                    "quantity", order.quantity().toString(),
                    "amount", order.amount().toString()), now));

            return order.id();

        } catch (InvalidOrderException rejection) {
            // O pedido rejeitado também é um fato observável — e é justamente o que
            // o App C vai destacar quando o k6 mandar payload inválido de propósito.
            activityLog.publish(ActivityLog.warn("order.rejected", ActivityLog.context(
                    "violations", rejection.violations().stream()
                            .map(Violation::toString).collect(Collectors.joining("; ")),
                    "product", String.valueOf(command.product())), now));
            throw rejection;
        }
    }
}
