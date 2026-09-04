package dev.joaolaureano.trainingkafka.orders.adapters.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.OrderEventOutboxTranslator;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderStatus;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQLite de verdade, em memória: o que se está testando é justamente o SQL — a
 * atomicidade entre o pedido e o seu outbox não existiria num fake.
 */
class SqliteOrderRepositoryTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    private Connection connection;
    private SqliteOrderRepository repository;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        connection.setAutoCommit(false);
        repository = new SqliteOrderRepository(connection,
                new OrderEventOutboxTranslator(new ObjectMapper()));
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    @DisplayName("grava o pedido e a linha de outbox no mesmo commit")
    void persistsOrderAndOutboxTogether() {
        Order order = anOrder();

        repository.save(order, order.pullDomainEvents());

        assertThat(repository.findById(order.id())).get()
                .satisfies(loaded -> assertThat(loaded.status()).isEqualTo(OrderStatus.PENDING_STOCK));
        assertThat(repository.pending(10)).singleElement().satisfies(record -> {
            assertThat(record.topic()).isEqualTo("orders");
            assertThat(record.eventType()).isEqualTo("OrderPlaced");
            assertThat(record.partitionKey()).isEqualTo("cust-1");
            assertThat(record.payload()).contains(order.id().toString());
        });
    }

    @Test
    @DisplayName("evento sem tradução aborta a transação inteira: nem pedido, nem outbox")
    void untranslatableEventRollsEverythingBack() {
        SqliteOrderRepository failing = new SqliteOrderRepository(connection, event -> {
            throw new IllegalArgumentException("sem tradução");
        });
        Order order = anOrder();

        assertThatThrownBy(() -> failing.save(order, order.pullDomainEvents()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(failing.findById(order.id())).isEmpty();
        assertThat(failing.pending(10)).isEmpty();
    }

    @Test
    @DisplayName("a mudança de status não reenfileira o pedido no outbox")
    void statusUpdateDoesNotDuplicateOutbox() {
        Order order = anOrder();
        repository.save(order, order.pullDomainEvents());

        Order loaded = repository.findById(order.id()).orElseThrow();
        loaded.confirmStock();
        loaded.approvePayment();
        repository.save(loaded);

        assertThat(repository.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.PAID);
        assertThat(repository.pending(10)).hasSize(1);
    }

    /**
     * A corrida que os dois escritores do pedido criam.
     *
     * Estoque e pagamento chegam por tópicos diferentes, em threads diferentes, e
     * escrevem no mesmo agregado. Com {@code findById} seguido de {@code save} na
     * camada de aplicação, os dois liam PENDING_STOCK, um gravava PAID e o outro
     * gravava PENDING_PAYMENT por cima — o pedido ficava preso para sempre, sem
     * exceção e sem DLQ.
     *
     * <p>Duas threads em corrida livre NÃO servem para testar isso: a janela entre
     * ler e gravar é de microssegundos, e centenas de rodadas passam sem acertá-la
     * (verificado — o teste passava com a atomicidade removida). O que se afirma
     * aqui é a propriedade que fecha a janela, e ela é determinística: enquanto uma
     * transição está em curso, nenhum outro escritor consegue sequer LER o pedido.
     */
    @Test
    @DisplayName("uma transição em curso bloqueia o outro escritor até commitar")
    void applyTransitionIsExclusive() throws Exception {
        Order order = anOrder();
        repository.save(order, order.pullDomainEvents());

        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<OrderStatus> seenByTheOtherWriter = new AtomicReference<>();

        Thread holder = new Thread(() -> repository.applyTransition(order.id(), loaded -> {
            loaded.confirmStock();
            inside.countDown();
            await(release);
        }));
        holder.start();
        inside.await();

        Thread other = new Thread(() -> seenByTheOtherWriter.set(
                repository.findById(order.id()).orElseThrow().status()));
        other.start();
        other.join(500);
        boolean blockedWhileInFlight = other.isAlive();

        release.countDown();
        holder.join();
        other.join();

        assertThat(blockedWhileInFlight)
                .as("ler no meio da transição é exatamente o que produz o lost update")
                .isTrue();
        assertThat(seenByTheOtherWriter.get())
                .as("e o que ele lê depois é o estado já commitado")
                .isEqualTo(OrderStatus.PENDING_PAYMENT);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    @DisplayName("linha confirmada some do pendente e não volta")
    void publishedRowIsNotRedelivered() {
        Order order = anOrder();
        repository.save(order, order.pullDomainEvents());
        List<OutboxRecord> pending = repository.pending(10);

        repository.markPublished(pending.getFirst().sequence());

        assertThat(repository.pending(10)).isEmpty();
    }

    private static Order anOrder() {
        return Order.place("cust-1", "Teclado", 2, new BigDecimal("199.90"), NOW);
    }
}
