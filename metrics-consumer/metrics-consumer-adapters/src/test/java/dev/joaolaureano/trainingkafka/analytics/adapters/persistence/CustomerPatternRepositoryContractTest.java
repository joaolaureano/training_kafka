package dev.joaolaureano.trainingkafka.analytics.adapters.persistence;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb.DuckDbCustomerPatternRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory.InMemoryCustomerPatternRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite.SqliteCustomerPatternRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.model.*;
import dev.joaolaureano.trainingkafka.analytics.domain.port.CustomerPatternRepository;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CustomerPatternRepositoryContractTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");
    private static final CustomerId CUSTOMER = new CustomerId("cust-1");
    private static final SuspicionPolicy POLICY = SuspicionPolicy.of(5, Duration.ofSeconds(10));

    static Stream<Arguments> implementations() {
        return Stream.of(
                Arguments.of("inmemory", (Supplier<CustomerPatternRepository>)
                        InMemoryCustomerPatternRepository::new),
                Arguments.of("sqlite", (Supplier<CustomerPatternRepository>)
                        () -> new SqliteCustomerPatternRepository(TestRepositories.freshSqlite())),
                Arguments.of("duckdb", (Supplier<CustomerPatternRepository>)
                        () -> new DuckDbCustomerPatternRepository(TestRepositories.freshDuckDb())));
    }

    @ParameterizedTest(name = "[{0}] cliente novo volta com janela vazia")
    @MethodSource("implementations")
    void newCustomerHasEmptyWindow(String name, Supplier<CustomerPatternRepository> factory) {
        CustomerPatternRepository repository = factory.get();

        CustomerOrderPattern pattern = repository.findOrCreate(CUSTOMER, POLICY);

        assertThat(pattern).isNotNull();
        assertThat(pattern.ordersInWindow()).isZero();
        assertThat(pattern.isSuspicious()).isFalse();
    }

    @ParameterizedTest(name = "[{0}] a janela persistida sobrevive ao recarregamento")
    @MethodSource("implementations")
    void windowSurvivesReload(String name, Supplier<CustomerPatternRepository> factory) {
        CustomerPatternRepository repository = factory.get();

        CustomerOrderPattern pattern = repository.findOrCreate(CUSTOMER, POLICY);
        for (int i = 0; i < 3; i++) {
            pattern.registerOrder(new OrderId(UUID.randomUUID()), T0.plusSeconds(i), Money.of("50.00"));
        }
        repository.save(pattern);

        assertThat(repository.findOrCreate(CUSTOMER, POLICY).ordersInWindow()).isEqualTo(3);
    }

    @ParameterizedTest(name = "[{0}] a suspeita atravessa um reinício do serviço")
    @MethodSource("implementations")
    void suspicionSurvivesRestart(String name, Supplier<CustomerPatternRepository> factory) {
        CustomerPatternRepository repository = factory.get();

        CustomerOrderPattern pattern = repository.findOrCreate(CUSTOMER, POLICY);
        for (int i = 0; i < 5; i++) {
            pattern.registerOrder(new OrderId(UUID.randomUUID()), T0.plusSeconds(i), Money.of("50.00"));
        }
        repository.save(pattern);

        assertThat(repository.findOrCreate(CUSTOMER, POLICY).isSuspicious()).isTrue();
    }

    @ParameterizedTest(name = "[{0}] save substitui a janela, não acumula linhas antigas")
    @MethodSource("implementations")
    void saveReplacesWindow(String name, Supplier<CustomerPatternRepository> factory) {
        CustomerPatternRepository repository = factory.get();

        // Muitos pedidos espaçados: o agregado se poda a cada registro, então a
        // janela persistida tem que encolher junto — e não virar um histórico infinito.
        CustomerOrderPattern pattern = repository.findOrCreate(CUSTOMER, POLICY);
        for (int i = 0; i < 50; i++) {
            pattern.registerOrder(new OrderId(UUID.randomUUID()), T0.plusSeconds(i * 5L), Money.of("50.00"));
            repository.save(pattern);
        }

        assertThat(repository.findOrCreate(CUSTOMER, POLICY).ordersInWindow()).isLessThanOrEqualTo(3);
    }

    @ParameterizedTest(name = "[{0}] a policy vem do parâmetro, nunca do banco")
    @MethodSource("implementations")
    void policyIsNeverPersisted(String name, Supplier<CustomerPatternRepository> factory) {
        CustomerPatternRepository repository = factory.get();

        CustomerOrderPattern pattern = repository.findOrCreate(CUSTOMER, POLICY);
        for (int i = 0; i < 5; i++) {
            pattern.registerOrder(new OrderId(UUID.randomUUID()), T0.plusSeconds(i), Money.of("50.00"));
        }
        repository.save(pattern);

        // Mesmo dado, limiar diferente: mudar o application.yml tem que valer na hora.
        SuspicionPolicy lenient = SuspicionPolicy.of(50, Duration.ofSeconds(10));
        assertThat(repository.findOrCreate(CUSTOMER, lenient).isSuspicious()).isFalse();
    }

    @ParameterizedTest(name = "[{0}] clientes não se misturam")
    @MethodSource("implementations")
    void customersAreIsolated(String name, Supplier<CustomerPatternRepository> factory) {
        CustomerPatternRepository repository = factory.get();

        CustomerOrderPattern first = repository.findOrCreate(CUSTOMER, POLICY);
        first.registerOrder(new OrderId(UUID.randomUUID()), T0, Money.of("50.00"));
        repository.save(first);

        assertThat(repository.findOrCreate(new CustomerId("outro-cliente"), POLICY).ordersInWindow()).isZero();
    }

    @ParameterizedTest(name = "[{0}] recarregar não ressuscita eventos já despachados")
    @MethodSource("implementations")
    void reloadDoesNotReplayEvents(String name, Supplier<CustomerPatternRepository> factory) {
        CustomerPatternRepository repository = factory.get();

        CustomerOrderPattern pattern = repository.findOrCreate(CUSTOMER, POLICY);
        for (int i = 0; i < 5; i++) {
            pattern.registerOrder(new OrderId(UUID.randomUUID()), T0.plusSeconds(i), Money.of("50.00"));
        }
        pattern.pullDomainEvents();
        repository.save(pattern);

        // Reconstituir é restaurar estado, não reviver fatos: um alerta já
        // publicado não pode ser publicado de novo só porque o serviço reiniciou.
        assertThat(repository.findOrCreate(CUSTOMER, POLICY).pendingEvents()).isEmpty();
    }
}
