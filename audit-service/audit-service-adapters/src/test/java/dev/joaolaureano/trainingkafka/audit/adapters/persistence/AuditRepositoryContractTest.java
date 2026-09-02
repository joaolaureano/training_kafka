package dev.joaolaureano.trainingkafka.audit.adapters.persistence;

import dev.joaolaureano.trainingkafka.audit.adapters.persistence.duckdb.DuckDbAuditRepository;
import dev.joaolaureano.trainingkafka.audit.adapters.persistence.jsonl.JsonlFileAuditRepository;
import dev.joaolaureano.trainingkafka.audit.domain.model.*;
import dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A mesma bateria contra as implementações QUE ARMAZENAM.
 *
 * O adapter de stdout fica de fora de propósito: ele não guarda nada, então não
 * há o que consultar. Sua limitação é verificada separadamente, em
 * {@link StdoutAuditRepositoryTest} — o que importa é que ela seja uma
 * característica declarada e testada, e não uma surpresa em produção.
 */
class AuditRepositoryContractTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");

    static Stream<Arguments> implementations() {
        return Stream.of(
                Arguments.of("jsonl", (Function<Path, AuditRepository>)
                        dir -> new JsonlFileAuditRepository(dir.resolve("audit.jsonl"))),
                Arguments.of("duckdb", (Function<Path, AuditRepository>)
                        dir -> new DuckDbAuditRepository(openDuckDb(dir.resolve("audit.duckdb")))));
    }

    private static java.sql.Connection openDuckDb(Path path) {
        try {
            return DriverManager.getConnection("jdbc:duckdb:" + path);
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static AuditEvent entry(AuditLevel level, String app, String action, long secondsFromT0) {
        return new AuditEvent(level, T0.plusSeconds(secondsFromT0), new ApplicationName(app),
            action, Map.of());
    }

    @ParameterizedTest(name = "[{0}] repositório vazio devolve lista vazia")
    @MethodSource("implementations")
    void emptyRepository(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        assertThat(factory.apply(dir).query(AuditFilter.all(), 10)).isEmpty();
    }

    @ParameterizedTest(name = "[{0}] o que foi salvo pode ser lido de volta, íntegro")
    @MethodSource("implementations")
    void savesAndReads(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        repository.save(entry(AuditLevel.WARN, "order-service", "order.rejected", 0));

        assertThat(repository.query(AuditFilter.all(), 10)).singleElement().satisfies(found -> {
            assertThat(found.level()).isEqualTo(AuditLevel.WARN);
            assertThat(found.app().value()).isEqualTo("order-service");
            assertThat(found.action()).isEqualTo("order.rejected");
            assertThat(found.occurredAt()).isEqualTo(T0);
            assertThat(found.context()).isEmpty();
        });
    }

    @ParameterizedTest(name = "[{0}] nível filtra por severidade mínima")
    @MethodSource("implementations")
    void filtersByMinimumLevel(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        repository.save(entry(AuditLevel.DEBUG, "app", "detalhe", 0));
        repository.save(entry(AuditLevel.INFO, "app", "normal", 1));
        repository.save(entry(AuditLevel.WARN, "app", "atenção", 2));
        repository.save(entry(AuditLevel.ERROR, "app", "falha", 3));

        assertThat(repository.query(AuditFilter.all().withMinimumLevel(AuditLevel.WARN), 10))
                .extracting(AuditEvent::action)
                .containsExactlyInAnyOrder("atenção", "falha");
    }

    @ParameterizedTest(name = "[{0}] filtra por aplicação")
    @MethodSource("implementations")
    void filtersByApp(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        repository.save(entry(AuditLevel.WARN, "order-service", "de A", 0));
        repository.save(entry(AuditLevel.WARN, "metrics-consumer", "de B", 1));

        assertThat(repository.query(AuditFilter.all().withApp(new ApplicationName("metrics-consumer")), 10))
                .extracting(AuditEvent::action)
                .containsExactly("de B");
    }

    @ParameterizedTest(name = "[{0}] filtra por período")
    @MethodSource("implementations")
    void filtersByRange(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        repository.save(entry(AuditLevel.INFO, "app", "dentro", 10));
        repository.save(entry(AuditLevel.INFO, "app", "fora", 5000));

        AuditFilter window = AuditFilter.all().withRange(new TimeRange(T0, T0.plusSeconds(60)));

        assertThat(repository.query(window, 10)).extracting(AuditEvent::action).containsExactly("dentro");
    }

    @ParameterizedTest(name = "[{0}] devolve do mais recente para o mais antigo")
    @MethodSource("implementations")
    void newestFirst(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        repository.save(entry(AuditLevel.INFO, "app", "primeiro", 0));
        repository.save(entry(AuditLevel.INFO, "app", "segundo", 10));
        repository.save(entry(AuditLevel.INFO, "app", "terceiro", 20));

        assertThat(repository.query(AuditFilter.all(), 10))
                .extracting(AuditEvent::action)
                .containsExactly("terceiro", "segundo", "primeiro");
    }

    @ParameterizedTest(name = "[{0}] respeita o limite")
    @MethodSource("implementations")
    void respectsLimit(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        for (int i = 0; i < 20; i++) {
            repository.save(entry(AuditLevel.INFO, "app", "msg-" + i, i));
        }

        assertThat(repository.query(AuditFilter.all(), 5)).hasSize(5);
    }

    @ParameterizedTest(name = "[{0}] critérios combinados aplicam todos juntos")
    @MethodSource("implementations")
    void combinedCriteria(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        repository.save(entry(AuditLevel.ERROR, "order-service", "erro em A", 0));
        repository.save(entry(AuditLevel.ERROR, "metrics-consumer", "erro em B", 1));
        repository.save(entry(AuditLevel.INFO, "metrics-consumer", "info em B", 2));

        AuditFilter filter = AuditFilter.all()
                .withMinimumLevel(AuditLevel.WARN)
                .withApp(new ApplicationName("metrics-consumer"));

        assertThat(repository.query(filter, 10)).extracting(AuditEvent::action).containsExactly("erro em B");
    }

    @ParameterizedTest(name = "[{0}] mensagens com aspas, quebras de linha e acentos sobrevivem")
    @MethodSource("implementations")
    void survivesAwkwardContent(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) {
        AuditRepository repository = factory.apply(dir);
        String nasty = "aspas \" barra \\ quebra \n tab \t acentuação çãõ";
        repository.save(new AuditEvent(AuditLevel.WARN, T0, new ApplicationName("app"), nasty,
                Map.of("json", "{\"nested\": \"value\"}")));

        assertThat(repository.query(AuditFilter.all(), 10)).singleElement().satisfies(found -> {
            assertThat(found.action()).isEqualTo(nasty);
            assertThat(found.context()).containsEntry("json", "{\"nested\": \"value\"}");
        });
    }

    @ParameterizedTest(name = "[{0}] os dados sobrevivem a uma nova instância do repositório")
    @MethodSource("implementations")
    void survivesReopen(String name, Function<Path, AuditRepository> factory, @TempDir Path dir) throws Exception {
        factory.apply(dir).save(entry(AuditLevel.WARN, "app", "persistido", 0));

        // Nova instância sobre o mesmo diretório: é o equivalente a reiniciar o serviço.
        assertThat(factory.apply(dir).query(AuditFilter.all(), 10))
                .extracting(AuditEvent::action)
                .containsExactly("persistido");
        assertThat(Files.list(dir).findAny()).isPresent();
    }
}
