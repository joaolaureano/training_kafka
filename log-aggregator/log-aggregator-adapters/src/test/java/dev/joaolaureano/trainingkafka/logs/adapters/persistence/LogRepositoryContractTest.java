package dev.joaolaureano.trainingkafka.logs.adapters.persistence;

import dev.joaolaureano.trainingkafka.logs.adapters.persistence.duckdb.DuckDbLogRepository;
import dev.joaolaureano.trainingkafka.logs.adapters.persistence.jsonl.JsonlFileLogRepository;
import dev.joaolaureano.trainingkafka.logs.domain.model.*;
import dev.joaolaureano.trainingkafka.logs.domain.port.LogRepository;
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
 * {@link StdoutLogRepositoryTest} — o que importa é que ela seja uma
 * característica declarada e testada, e não uma surpresa em produção.
 */
class LogRepositoryContractTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");

    static Stream<Arguments> implementations() {
        return Stream.of(
                Arguments.of("jsonl", (Function<Path, LogRepository>)
                        dir -> new JsonlFileLogRepository(dir.resolve("logs.jsonl"))),
                Arguments.of("duckdb", (Function<Path, LogRepository>)
                        dir -> new DuckDbLogRepository(openDuckDb(dir.resolve("logs.duckdb")))));
    }

    private static java.sql.Connection openDuckDb(Path path) {
        try {
            return DriverManager.getConnection("jdbc:duckdb:" + path);
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static LogEntry entry(LogLevel level, String app, String message, long secondsFromT0) {
        return new LogEntry(level, T0.plusSeconds(secondsFromT0), new ApplicationName(app),
                message, Map.of("customerId", "cust-1"));
    }

    @ParameterizedTest(name = "[{0}] repositório vazio devolve lista vazia")
    @MethodSource("implementations")
    void emptyRepository(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        assertThat(factory.apply(dir).query(LogFilter.all(), 10)).isEmpty();
    }

    @ParameterizedTest(name = "[{0}] o que foi salvo pode ser lido de volta, íntegro")
    @MethodSource("implementations")
    void savesAndReads(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        repository.save(entry(LogLevel.WARN, "order-service", "order rejected", 0));

        assertThat(repository.query(LogFilter.all(), 10)).singleElement().satisfies(found -> {
            assertThat(found.level()).isEqualTo(LogLevel.WARN);
            assertThat(found.app().value()).isEqualTo("order-service");
            assertThat(found.message()).isEqualTo("order rejected");
            assertThat(found.occurredAt()).isEqualTo(T0);
            assertThat(found.context()).containsEntry("customerId", "cust-1");
        });
    }

    @ParameterizedTest(name = "[{0}] nível filtra por severidade mínima")
    @MethodSource("implementations")
    void filtersByMinimumLevel(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        repository.save(entry(LogLevel.DEBUG, "app", "detalhe", 0));
        repository.save(entry(LogLevel.INFO, "app", "normal", 1));
        repository.save(entry(LogLevel.WARN, "app", "atenção", 2));
        repository.save(entry(LogLevel.ERROR, "app", "falha", 3));

        assertThat(repository.query(LogFilter.all().withMinimumLevel(LogLevel.WARN), 10))
                .extracting(LogEntry::message)
                .containsExactlyInAnyOrder("atenção", "falha");
    }

    @ParameterizedTest(name = "[{0}] filtra por aplicação")
    @MethodSource("implementations")
    void filtersByApp(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        repository.save(entry(LogLevel.WARN, "order-service", "de A", 0));
        repository.save(entry(LogLevel.WARN, "metrics-consumer", "de B", 1));

        assertThat(repository.query(LogFilter.all().withApp(new ApplicationName("metrics-consumer")), 10))
                .extracting(LogEntry::message)
                .containsExactly("de B");
    }

    @ParameterizedTest(name = "[{0}] filtra por período")
    @MethodSource("implementations")
    void filtersByRange(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        repository.save(entry(LogLevel.INFO, "app", "dentro", 10));
        repository.save(entry(LogLevel.INFO, "app", "fora", 5000));

        LogFilter window = LogFilter.all().withRange(new TimeRange(T0, T0.plusSeconds(60)));

        assertThat(repository.query(window, 10)).extracting(LogEntry::message).containsExactly("dentro");
    }

    @ParameterizedTest(name = "[{0}] devolve do mais recente para o mais antigo")
    @MethodSource("implementations")
    void newestFirst(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        repository.save(entry(LogLevel.INFO, "app", "primeiro", 0));
        repository.save(entry(LogLevel.INFO, "app", "segundo", 10));
        repository.save(entry(LogLevel.INFO, "app", "terceiro", 20));

        assertThat(repository.query(LogFilter.all(), 10))
                .extracting(LogEntry::message)
                .containsExactly("terceiro", "segundo", "primeiro");
    }

    @ParameterizedTest(name = "[{0}] respeita o limite")
    @MethodSource("implementations")
    void respectsLimit(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        for (int i = 0; i < 20; i++) {
            repository.save(entry(LogLevel.INFO, "app", "msg-" + i, i));
        }

        assertThat(repository.query(LogFilter.all(), 5)).hasSize(5);
    }

    @ParameterizedTest(name = "[{0}] critérios combinados aplicam todos juntos")
    @MethodSource("implementations")
    void combinedCriteria(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        repository.save(entry(LogLevel.ERROR, "order-service", "erro em A", 0));
        repository.save(entry(LogLevel.ERROR, "metrics-consumer", "erro em B", 1));
        repository.save(entry(LogLevel.INFO, "metrics-consumer", "info em B", 2));

        LogFilter filter = LogFilter.all()
                .withMinimumLevel(LogLevel.WARN)
                .withApp(new ApplicationName("metrics-consumer"));

        assertThat(repository.query(filter, 10)).extracting(LogEntry::message).containsExactly("erro em B");
    }

    @ParameterizedTest(name = "[{0}] mensagens com aspas, quebras de linha e acentos sobrevivem")
    @MethodSource("implementations")
    void survivesAwkwardContent(String name, Function<Path, LogRepository> factory, @TempDir Path dir) {
        LogRepository repository = factory.apply(dir);
        String nasty = "aspas \" barra \\ quebra \n tab \t acentuação çãõ";
        repository.save(new LogEntry(LogLevel.WARN, T0, new ApplicationName("app"), nasty,
                Map.of("json", "{\"nested\": \"value\"}")));

        assertThat(repository.query(LogFilter.all(), 10)).singleElement().satisfies(found -> {
            assertThat(found.message()).isEqualTo(nasty);
            assertThat(found.context()).containsEntry("json", "{\"nested\": \"value\"}");
        });
    }

    @ParameterizedTest(name = "[{0}] os dados sobrevivem a uma nova instância do repositório")
    @MethodSource("implementations")
    void survivesReopen(String name, Function<Path, LogRepository> factory, @TempDir Path dir) throws Exception {
        factory.apply(dir).save(entry(LogLevel.WARN, "app", "persistido", 0));

        // Nova instância sobre o mesmo diretório: é o equivalente a reiniciar o serviço.
        assertThat(factory.apply(dir).query(LogFilter.all(), 10))
                .extracting(LogEntry::message)
                .containsExactly("persistido");
        assertThat(Files.list(dir).findAny()).isPresent();
    }
}
