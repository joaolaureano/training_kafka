package dev.joaolaureano.trainingkafka.payment.application;

import dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentApproved;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentFailed;
import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.model.PaymentStatus;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Sem Mockito: os Ports são pequenos o bastante para fakes de cinco linhas. */
class ProcessOrderPaymentTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private final FakeRepository repository = new FakeRepository();

    @Test
    @DisplayName("aprovação do gateway vira PaymentApproved")
    void approvedChargePublishesApproval() {
        service(approving()).handle("order-1", "cust-1", BigDecimal.TEN, "corr-1");

        assertThat(repository.stored.get("order-1").status()).isEqualTo(PaymentStatus.APPROVED);
        assertThat(repository.events).singleElement()
                .isInstanceOfSatisfying(PaymentApproved.class, event -> {
                    assertThat(event.orderId()).isEqualTo("order-1");
                    assertThat(event.correlationId()).isEqualTo("corr-1");
                    assertThat(event.occurredAt()).isEqualTo(NOW);
                });
    }

    @Test
    @DisplayName("recusa do gateway vira PaymentFailed — não é erro, é desfecho")
    void declinedChargePublishesFailure() {
        service(declining("limite excedido")).handle("order-1", "cust-1", BigDecimal.TEN, "corr-1");

        assertThat(repository.stored.get("order-1").status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(repository.events).singleElement()
                .isInstanceOfSatisfying(PaymentFailed.class, event ->
                        assertThat(event.reason()).isEqualTo("limite excedido"));
    }

    @Test
    @DisplayName("OrderPlaced reentregue não cobra de novo")
    void duplicateOrderIsChargedOnlyOnce() {
        RecordingGateway gateway = approving();
        ProcessOrderPayment service = service(gateway);

        service.handle("order-1", "cust-1", BigDecimal.TEN, "corr-1");
        service.handle("order-1", "cust-1", BigDecimal.TEN, "corr-1");

        assertThat(gateway.charges).isEqualTo(1);
        assertThat(repository.stored.get("order-1").status()).isEqualTo(PaymentStatus.APPROVED);
    }

    @Test
    @DisplayName("reentrega sobre pagamento já resolvido não cobra nem duplica evento")
    void redeliveryNeitherChargesNorRepublishes() {
        // Antes do outbox era preciso reemitir aqui, porque o crash entre gravar e
        // publicar perdia o evento. Agora o evento está na mesma transação do
        // desfecho: se o pagamento existe, o resultado dele já está a caminho.
        Payment settled = Payment.request("order-1", "cust-1", BigDecimal.TEN);
        settled.approve(NOW, "corr-1");
        settled.pullDomainEvents();
        repository.save(settled);

        RecordingGateway gateway = approving();
        service(gateway).handle("order-1", "cust-1", BigDecimal.TEN, "corr-1");

        assertThat(gateway.charges).isZero();
        assertThat(repository.events).isEmpty();
    }

    private ProcessOrderPayment service(PaymentGateway gateway) {
        return new ProcessOrderPayment(repository, gateway, CLOCK);
    }

    private static RecordingGateway approving() {
        return new RecordingGateway(PaymentGateway.GatewayResult.accepted());
    }

    private static RecordingGateway declining(String reason) {
        return new RecordingGateway(PaymentGateway.GatewayResult.declined(reason));
    }

    /**
     * O caso de uso não publica mais: grava o pagamento e seus eventos na mesma
     * chamada. O fake registra os dois lados para que o teste possa afirmar que o
     * evento foi enfileirado JUNTO do desfecho, e não depois dele.
     */
    private static final class FakeRepository implements PaymentRepository {
        private final Map<String, Payment> stored = new HashMap<>();
        private final List<DomainEvent> events = new ArrayList<>();

        @Override
        public Optional<Payment> findByOrderId(String orderId) {
            return Optional.ofNullable(stored.get(orderId));
        }

        @Override
        public void save(Payment payment, List<DomainEvent> newEvents) {
            stored.put(payment.orderId(), payment);
            events.addAll(newEvents);
        }

        @Override
        public void save(Payment payment) {
            save(payment, List.of());
        }
    }

    private static final class RecordingGateway implements PaymentGateway {
        private final GatewayResult result;
        private int charges;

        RecordingGateway(GatewayResult result) {
            this.result = result;
        }

        @Override
        public GatewayResult charge(Payment payment) {
            charges++;
            return result;
        }
    }

}
