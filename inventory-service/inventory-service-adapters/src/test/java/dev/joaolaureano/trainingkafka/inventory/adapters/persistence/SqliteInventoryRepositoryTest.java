package dev.joaolaureano.trainingkafka.inventory.adapters.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.InventoryEventOutboxTranslator;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.inventory.domain.model.RejectionReason;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Reservation;
import dev.joaolaureano.trainingkafka.inventory.domain.model.ReservationStatus;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;
import dev.joaolaureano.trainingkafka.inventory.domain.port.ConcurrentStockChangeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQLite de verdade, em memória: o que se está testando é justamente o SQL. Nem a
 * atomicidade entre estoque, reserva e outbox, nem o bloqueio otimista existiriam
 * num fake — os dois são propriedades do banco, não do objeto.
 */
class SqliteInventoryRepositoryTest {

    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");
    private static final Sku TECLADO = Sku.of("TECLADO");
    private static final BigDecimal AMOUNT = new BigDecimal("199.90");

    private Connection connection;
    private SqliteInventoryRepository repository;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        connection.setAutoCommit(false);
        repository = new SqliteInventoryRepository(connection,
                new InventoryEventOutboxTranslator(new ObjectMapper()));
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    private Product seed(int available) {
        repository.saveProduct(Product.define("TECLADO", "Teclado mecânico", available));
        return repository.findBySku(TECLADO).orElseThrow();
    }

    private static Reservation heldFor(String orderId, Quantity quantity) {
        return Reservation.held(orderId, TECLADO, quantity, "cust-1", AMOUNT, "corr-1", NOW);
    }

    @Test
    @DisplayName("upsert cria o produto e depois o redefine, sem duplicar")
    void upsertCreatesThenRedefines() {
        seed(10);
        repository.saveProduct(Product.define("TECLADO", "Teclado novo", 40));

        assertThat(repository.findAll()).singleElement().satisfies(product -> {
            assertThat(product.name()).isEqualTo("Teclado novo");
            assertThat(product.available().value()).isEqualTo(40);
        });
    }

    @Test
    @DisplayName("grava estoque, reserva e outbox no mesmo commit")
    void persistsStockReservationAndOutboxTogether() {
        Product product = seed(10);
        product.reserve(new Quantity(3));
        Reservation reservation = heldFor("order-1", new Quantity(3));

        repository.save(reservation, product, reservation.pullDomainEvents());

        assertThat(repository.findBySku(TECLADO).orElseThrow().available().value()).isEqualTo(7);
        assertThat(repository.findReservation("order-1")).get()
                .satisfies(loaded -> assertThat(loaded.status()).isEqualTo(ReservationStatus.HELD));
        assertThat(repository.pending(10)).singleElement().satisfies(record -> {
            assertThat(record.topic()).isEqualTo("inventory-events");
            assertThat(record.eventType()).isEqualTo("StockReserved");
            assertThat(record.partitionKey())
                    .as("a chave é o pedido, para todos os desfechos dele chegarem em ordem")
                    .isEqualTo("order-1");
        });
    }

    @Test
    @DisplayName("evento sem tradução aborta a transação inteira: nem estoque, nem reserva")
    void untranslatableEventRollsEverythingBack() {
        SqliteInventoryRepository failing = new SqliteInventoryRepository(connection, event -> {
            throw new IllegalArgumentException("sem tradução");
        });
        failing.saveProduct(Product.define("TECLADO", "Teclado mecânico", 10));
        Product product = failing.findBySku(TECLADO).orElseThrow();
        product.reserve(new Quantity(3));
        Reservation reservation = heldFor("order-1", new Quantity(3));

        assertThatThrownBy(() -> failing.save(reservation, product, reservation.pullDomainEvents()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(failing.findBySku(TECLADO).orElseThrow().available().value())
                .as("o estoque não pode ter sido descontado")
                .isEqualTo(10);
        assertThat(failing.findReservation("order-1")).isEmpty();
        assertThat(failing.pending(10)).isEmpty();
    }

    /**
     * A corrida que a funcionalidade inteira existe para não perder.
     *
     * Dois pedidos leem o mesmo produto com uma unidade e ambos passam por
     * {@code reserve} — nenhum agregado enxerga o outro. Quem grava primeiro vence;
     * o segundo encontra a linha numa versão que não é mais a sua e é recusado. Sem
     * isso, os dois commits passariam e o estoque terminaria negativo, ou zerado com
     * duas vendas registradas.
     */
    @Test
    @DisplayName("dois pedidos pela última unidade: um vence, o outro é recusado")
    void concurrentReservationsForTheLastUnit() {
        seed(1);

        Product forFirstOrder = repository.findBySku(TECLADO).orElseThrow();
        Product forSecondOrder = repository.findBySku(TECLADO).orElseThrow();

        forFirstOrder.reserve(new Quantity(1));
        forSecondOrder.reserve(new Quantity(1));

        Reservation first = heldFor("order-1", new Quantity(1));
        repository.save(first, forFirstOrder, first.pullDomainEvents());

        Reservation second = heldFor("order-2", new Quantity(1));
        assertThatThrownBy(() -> repository.save(second, forSecondOrder, second.pullDomainEvents()))
                .isInstanceOf(ConcurrentStockChangeException.class);

        assertThat(repository.findBySku(TECLADO).orElseThrow().available().value())
                .as("nunca pode ficar negativo")
                .isZero();
        assertThat(repository.findReservation("order-2"))
                .as("a reserva perdedora não pode ter sido gravada")
                .isEmpty();
        assertThat(repository.pending(10))
                .as("nem o evento dela")
                .hasSize(1);
    }

    @Test
    @DisplayName("depois de perder a corrida, reler mostra o estoque real")
    void rereadingAfterAConflictSeesTheTruth() {
        seed(1);
        Product stale = repository.findBySku(TECLADO).orElseThrow();
        Product winner = repository.findBySku(TECLADO).orElseThrow();
        winner.reserve(new Quantity(1));
        Reservation first = heldFor("order-1", new Quantity(1));
        repository.save(first, winner, first.pullDomainEvents());

        assertThat(stale.covers(new Quantity(1)))
                .as("o agregado velho ainda acredita que há estoque")
                .isTrue();
        assertThat(repository.findBySku(TECLADO).orElseThrow().covers(new Quantity(1)))
                .as("a releitura desfaz a ilusão — e é por isso que o caso de uso relê")
                .isFalse();
    }

    @Test
    @DisplayName("a rejeição é gravada sem tocar no estoque")
    void rejectionIsPersistedWithoutStockChange() {
        seed(5);
        Reservation rejected = Reservation.rejected("order-1", TECLADO, new Quantity(9), "cust-1",
                AMOUNT, "corr-1", RejectionReason.OUT_OF_STOCK, NOW);

        repository.save(rejected, rejected.pullDomainEvents());

        assertThat(repository.findBySku(TECLADO).orElseThrow().available().value()).isEqualTo(5);
        assertThat(repository.findReservation("order-1")).get().satisfies(loaded -> {
            assertThat(loaded.status()).isEqualTo(ReservationStatus.REJECTED);
            assertThat(loaded.reason()).isEqualTo("OUT_OF_STOCK");
        });
        assertThat(repository.pending(10)).singleElement()
                .satisfies(record -> assertThat(record.eventType()).isEqualTo("StockRejected"));
    }

    @Test
    @DisplayName("liberar devolve o estoque e muda o desfecho da mesma reserva")
    void releaseGivesTheStockBack() {
        Product product = seed(10);
        product.reserve(new Quantity(3));
        Reservation reservation = heldFor("order-1", new Quantity(3));
        repository.save(reservation, product, reservation.pullDomainEvents());

        Reservation loaded = repository.findReservation("order-1").orElseThrow();
        Product current = repository.findBySku(TECLADO).orElseThrow();
        current.release(loaded.quantity());
        loaded.release("PaymentFailed", NOW);
        repository.save(loaded, current, loaded.pullDomainEvents());

        assertThat(repository.findBySku(TECLADO).orElseThrow().available().value()).isEqualTo(10);
        assertThat(repository.findReservation("order-1").orElseThrow().status())
                .isEqualTo(ReservationStatus.RELEASED);
        assertThat(repository.pending(10))
                .as("a reserva não pode ter virado duas linhas na tabela")
                .hasSize(2)
                .extracting(OutboxRecord::eventType)
                .containsExactly("StockReserved", "StockReleased");
    }

    @Test
    @DisplayName("a lápide é gravada sem SKU e é encontrada pela busca por pedido")
    void voidedReservationRoundTrips() {
        Reservation voided = Reservation.voided("order-1", "cust-1", AMOUNT, "corr-1",
                "PaymentCancelled", NOW);

        repository.save(voided, voided.pullDomainEvents());

        assertThat(repository.findReservation("order-1")).get().satisfies(loaded -> {
            assertThat(loaded.status()).isEqualTo(ReservationStatus.VOIDED);
            assertThat(loaded.sku()).isNull();
            assertThat(loaded.isHeld()).isFalse();
        });
        assertThat(repository.pending(10))
                .as("nada se moveu, então não há fato a publicar")
                .isEmpty();
    }

    @Test
    @DisplayName("linha confirmada some do pendente e não volta")
    void publishedRowIsNotRedelivered() {
        Product product = seed(10);
        product.reserve(new Quantity(1));
        Reservation reservation = heldFor("order-1", new Quantity(1));
        repository.save(reservation, product, reservation.pullDomainEvents());

        List<OutboxRecord> pending = repository.pending(10);
        repository.markPublished(pending.get(0).sequence());

        assertThat(repository.pending(10)).isEmpty();
    }

    @Test
    @DisplayName("o ajuste de catálogo invalida uma decisão em voo, em vez de ser sobrescrito")
    void catalogUpsertBeatsAnInFlightDecision() {
        Product inFlight = seed(10);
        inFlight.reserve(new Quantity(2));

        repository.saveProduct(Product.define("TECLADO", "Teclado mecânico", 3));

        Reservation reservation = heldFor("order-1", new Quantity(2));
        assertThatThrownBy(() -> repository.save(reservation, inFlight, reservation.pullDomainEvents()))
                .isInstanceOf(ConcurrentStockChangeException.class);
        assertThat(repository.findBySku(TECLADO).orElseThrow().available().value()).isEqualTo(3);
    }
}
