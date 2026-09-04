package dev.joaolaureano.trainingkafka.inventory.application;

import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.inventory.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockRejected;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockReserved;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.inventory.domain.model.RejectionReason;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Reservation;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;
import dev.joaolaureano.trainingkafka.inventory.domain.port.ConcurrentStockChangeException;
import dev.joaolaureano.trainingkafka.inventory.domain.port.InventoryRepository;
import org.junit.jupiter.api.BeforeEach;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * O que se testa aqui é o que o agregado sozinho não pode saber: que a mesma
 * mensagem chega duas vezes, e que outro consumidor pode ter chegado antes.
 */
class ReserveStockForOrderTest {

    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");
    private static final BigDecimal AMOUNT = new BigDecimal("199.90");

    private FakeInventory inventory;
    private RecordingLogPublisher logs;
    private ReserveStockForOrder reserveStock;

    @BeforeEach
    void setUp() {
        inventory = new FakeInventory();
        logs = new RecordingLogPublisher();
        reserveStock = new ReserveStockForOrder(inventory, logs,
                Clock.fixed(NOW, ZoneOffset.UTC), 5);
    }

    private void seed(int available) {
        inventory.products.put("TECLADO",
                Product.define("TECLADO", "Teclado mecânico", available));
    }

    private void place(String orderId, int quantity) {
        reserveStock.handle(orderId, "cust-1", "TECLADO", quantity, AMOUNT, "corr-1");
    }

    @Test
    @DisplayName("com estoque, separa as unidades e anuncia a reserva")
    void reservesWhenThereIsStock() {
        seed(10);

        place("order-1", 3);

        assertThat(inventory.products.get("TECLADO").available().value()).isEqualTo(7);
        assertThat(inventory.published).singleElement().isInstanceOf(StockReserved.class);
    }

    @Test
    @DisplayName("sem estoque, rejeita — e não toca no estoque")
    void rejectsWhenOutOfStock() {
        seed(2);

        place("order-1", 5);

        assertThat(inventory.products.get("TECLADO").available().value()).isEqualTo(2);
        assertThat(inventory.published).singleElement()
                .isInstanceOfSatisfying(StockRejected.class, event ->
                        assertThat(event.reason()).isEqualTo(RejectionReason.OUT_OF_STOCK));
    }

    @Test
    @DisplayName("produto fora do catálogo é rejeitado por outro motivo, não pelo mesmo")
    void rejectsUnknownProduct() {
        place("order-1", 1);

        assertThat(inventory.published).singleElement()
                .isInstanceOfSatisfying(StockRejected.class, event ->
                        assertThat(event.reason()).isEqualTo(RejectionReason.UNKNOWN_PRODUCT));
    }

    @Test
    @DisplayName("a mesma mensagem entregue duas vezes desconta o estoque uma vez só")
    void redeliveryDoesNotReserveTwice() {
        seed(10);

        place("order-1", 3);
        place("order-1", 3);

        assertThat(inventory.products.get("TECLADO").available().value()).isEqualTo(7);
        assertThat(inventory.published)
                .as("nem o pedido pode receber um segundo StockReserved")
                .hasSize(1);
    }

    @Test
    @DisplayName("reentrega de um pedido já rejeitado não é redecidida")
    void redeliveryOfARejectionIsNotReconsidered() {
        seed(0);
        place("order-1", 1);

        // O estoque foi reposto entre uma entrega e outra.
        seed(50);
        place("order-1", 1);

        assertThat(inventory.published)
                .as("mudar de ideia contradiria o cancelamento que o pedido já recebeu")
                .hasSize(1);
        assertThat(inventory.products.get("TECLADO").available().value()).isEqualTo(50);
    }

    @Test
    @DisplayName("compensação que chega antes do pedido impede a reserva de acontecer depois")
    void aVoidedOrderIsNeverReserved() {
        seed(10);
        ReleaseStockForOrder releaseStock = new ReleaseStockForOrder(inventory, logs,
                Clock.fixed(NOW, ZoneOffset.UTC), 5);

        // A fraude cancelou um pagamento que ainda não existia, e o PaymentCancelled
        // chegou antes do OrderPlaced.
        releaseStock.handle("order-1", "cust-1", AMOUNT, "corr-1", "PaymentCancelled");
        place("order-1", 3);

        assertThat(inventory.products.get("TECLADO").available().value())
                .as("reservar aqui prenderia estoque de um pedido já cancelado, para sempre")
                .isEqualTo(10);
        assertThat(inventory.published)
                .as("e o pedido morto não pode receber um StockReserved")
                .isEmpty();
    }

    @Test
    @DisplayName("perder a corrida faz reler e decidir de novo, não repetir a decisão velha")
    void retriesAfterLosingTheRace() {
        seed(10);
        // A primeira gravação perde para um concorrente que levou tudo.
        inventory.failNextSaveWith(() -> inventory.products.put("TECLADO",
                Product.define("TECLADO", "Teclado mecânico", 0)));

        place("order-1", 3);

        assertThat(inventory.published)
                .as("a releitura mostrou que não havia mais estoque")
                .singleElement()
                .isInstanceOfSatisfying(StockRejected.class, event ->
                        assertThat(event.reason()).isEqualTo(RejectionReason.OUT_OF_STOCK));
    }

    @Test
    @DisplayName("disputa que não cede devolve a mensagem ao container em vez de girar para sempre")
    void givesUpAfterTooManyConflicts() {
        seed(10);
        inventory.alwaysConflict = true;

        assertThatThrownBy(() -> place("order-1", 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("disputa");
    }

    private static final class FakeInventory implements InventoryRepository {

        private final Map<String, Product> products = new HashMap<>();
        private final Map<String, Reservation> reservations = new HashMap<>();
        private final List<DomainEvent> published = new ArrayList<>();
        private boolean alwaysConflict;
        private Runnable conflictOnce;

        void failNextSaveWith(Runnable sideEffect) {
            this.conflictOnce = sideEffect;
        }

        @Override
        public Optional<Product> findBySku(Sku sku) {
            // Devolve uma cópia: o repositório real reconstitui do banco, e um caso de
            // uso que mutasse o objeto compartilhado esconderia o conflito de versão.
            return Optional.ofNullable(products.get(sku.value()))
                    .map(p -> Product.reconstitute(p.sku(), p.name(),
                            new Quantity(p.available().value()), p.version()));
        }

        @Override
        public List<Product> findAll() {
            return List.copyOf(products.values());
        }

        @Override
        public Optional<Reservation> findReservation(String orderId) {
            return Optional.ofNullable(reservations.get(orderId));
        }

        @Override
        public void saveProduct(Product product) {
            products.put(product.sku().value(), product);
        }

        @Override
        public void save(Reservation reservation, Product product, List<DomainEvent> events) {
            if (alwaysConflict) {
                throw new ConcurrentStockChangeException("sempre em disputa");
            }
            if (conflictOnce != null) {
                Runnable sideEffect = conflictOnce;
                conflictOnce = null;
                sideEffect.run();
                throw new ConcurrentStockChangeException("perdeu a corrida");
            }
            products.put(product.sku().value(), product);
            save(reservation, events);
        }

        @Override
        public void save(Reservation reservation, List<DomainEvent> events) {
            reservations.put(reservation.orderId(), reservation);
            published.addAll(events);
        }
    }

    private static final class RecordingLogPublisher implements ActivityLogPublisher {
        private final List<ActivityLog> logs = new ArrayList<>();

        @Override
        public void publish(ActivityLog log) {
            logs.add(log);
        }
    }
}
