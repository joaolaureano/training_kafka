package dev.joaolaureano.trainingkafka.logs.adapters.persistence;

import dev.joaolaureano.trainingkafka.logs.adapters.persistence.stdout.StdoutLogRepository;
import dev.joaolaureano.trainingkafka.logs.domain.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O adapter de stdout fica fora do teste de contrato porque não armazena nada.
 *
 * Isso não é uma brecha: é uma limitação declarada na própria interface, e este
 * teste existe para fixá-la. Se um dia alguém "consertar" o query() para devolver
 * algo, este teste quebra e força a conversa.
 */
class StdoutLogRepositoryTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");

    private PrintStream originalOut;
    private ByteArrayOutputStream captured;

    @BeforeEach
    void redirectStdout() {
        originalOut = System.out;
        captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void restoreStdout() {
        System.setOut(originalOut);
    }

    private String output() {
        return captured.toString(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("imprime nível, aplicação e mensagem")
    void printsTheEssentials() {
        new StdoutLogRepository().save(new LogEntry(LogLevel.WARN, T0,
                new ApplicationName("metrics-consumer"), "suspicious order pattern detected",
                Map.of("customerId", "cust-1")));

        assertThat(output())
                .contains("WARN")
                .contains("metrics-consumer")
                .contains("suspicious order pattern detected")
                .contains("cust-1");
    }

    @Test
    @DisplayName("contexto vazio não imprime chaves vazias")
    void omitsEmptyContext() {
        new StdoutLogRepository().save(new LogEntry(LogLevel.INFO, T0,
                new ApplicationName("order-service"), "order accepted", Map.of()));

        assertThat(output()).doesNotContain("{}");
    }

    @Test
    @DisplayName("query devolve vazio porque não há onde procurar — não porque nada casou")
    void queryIsAlwaysEmptyByDesign() {
        StdoutLogRepository repository = new StdoutLogRepository();
        repository.save(new LogEntry(LogLevel.ERROR, T0, new ApplicationName("app"), "falha", Map.of()));

        assertThat(repository.query(LogFilter.all(), 100)).isEmpty();
    }
}
