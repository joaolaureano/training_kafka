package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.orders.application.port.AuditLevel;
import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.event.OrderPlaced;
import dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderException;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Sem Mockito: os Ports são interfaces pequenas o bastante para que um fake de
 * cinco linhas seja mais legível — e mais honesto — que um mock configurado.
 * Se um Port ficar difícil de fingir, é sinal de que ele está grande demais.
 */
class PlaceOrderServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-25T12:00:00Z");

    private RecordingEventPublisher events;
    private RecordingLogPublisher logs;
    private PlaceOrderService service;

    @BeforeEach
    void setUp() {
        events = new RecordingEventPublisher();
        logs = new RecordingLogPublisher();
        service = new PlaceOrderService(events, logs, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("publica OrderPlaced e devolve a identidade gerada")
    void publishesEvent() {
        var orderId = service.handle(new PlaceOrderCommand("cust-1", "Teclado", 2, new BigDecimal("199.90")));

        assertThat(events.published).singleElement()
                .isInstanceOfSatisfying(OrderPlaced.class, event -> {
                    assertThat(event.orderId()).isEqualTo(orderId);
                    assertThat(event.customerId().value()).isEqualTo("cust-1");
                    assertThat(event.quantity().value()).isEqualTo(2);
                    assertThat(event.occurredAt()).isEqualTo(NOW);
                });
    }

    @Test
    @DisplayName("registra log INFO para pedido aceito")
    void logsAcceptance() {
        service.handle(new PlaceOrderCommand("cust-1", "Teclado", 2, new BigDecimal("199.90")));

        assertThat(logs.published).singleElement().satisfies(log -> {
            assertThat(log.level()).isEqualTo(AuditLevel.INFO);
            assertThat(log.action()).isEqualTo("order.accepted");
            assertThat(log.context()).doesNotContainKey("customerId")
                    .containsEntry("amount", "199.90")
                    .containsKey("orderId");
        });
    }

    @Test
    @DisplayName("registra log WARN e propaga a exceção quando o domínio recusa")
    void logsRejection() {
        assertThatThrownBy(() -> service.handle(
                new PlaceOrderCommand("cust-1", "Teclado", -1, new BigDecimal("199.90"))))
                .isInstanceOf(InvalidOrderException.class);

        assertThat(logs.published).singleElement().satisfies(log -> {
            assertThat(log.level()).isEqualTo(AuditLevel.WARN);
            assertThat(log.action()).isEqualTo("order.rejected");
            assertThat(log.context()).doesNotContainKey("customerId");
            assertThat(log.context().get("violations")).contains("quantity");
        });
    }

    @Test
    @DisplayName("log de recusa lista todas as violações de uma vez")
    void logsEveryViolation() {
        assertThatThrownBy(() -> service.handle(
                new PlaceOrderCommand("", "", -1, new BigDecimal("-1"))))
                .isInstanceOf(InvalidOrderException.class);

        assertThat(logs.published.getFirst().context().get("violations"))
                .contains("customerId")
                .contains("product")
                .contains("quantity")
                .contains("amount");
    }

    @Test
    @DisplayName("pedido recusado não produz evento algum no tópico de pedidos")
    void rejectedOrderPublishesNoEvent() {
        assertThatThrownBy(() -> service.handle(
                new PlaceOrderCommand("", "Teclado", 1, new BigDecimal("10.00"))))
                .isInstanceOf(InvalidOrderException.class);

        assertThat(events.published).isEmpty();
    }

    @Test
    @DisplayName("usa o relógio injetado, nunca Instant.now()")
    void usesInjectedClock() {
        service.handle(new PlaceOrderCommand("cust-1", "Teclado", 1, new BigDecimal("10.00")));

        assertThat(((OrderPlaced) events.published.getFirst()).occurredAt()).isEqualTo(NOW);
        assertThat(logs.published.getFirst().occurredAt()).isEqualTo(NOW);
    }

    private static final class RecordingEventPublisher implements OrderEventPublisher {
        private final List<DomainEvent> published = new ArrayList<>();

        @Override
        public void publish(DomainEvent event) {
            published.add(event);
        }
    }

    private static final class RecordingLogPublisher implements ActivityLogPublisher {
        private final List<ActivityLog> published = new ArrayList<>();

        @Override
        public void publish(ActivityLog log) {
            published.add(log);
        }
    }
}
