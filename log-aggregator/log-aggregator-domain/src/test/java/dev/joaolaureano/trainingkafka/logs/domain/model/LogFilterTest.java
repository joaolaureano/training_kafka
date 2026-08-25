package dev.joaolaureano.trainingkafka.logs.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogFilterTest {

    private static final Instant NOW = Instant.parse("2026-08-25T12:00:00Z");

    private static LogEntry entry(LogLevel level, String app, Instant at) {
        return new LogEntry(level, at, new ApplicationName(app), "algo aconteceu", Map.of("k", "v"));
    }

    @Test
    @DisplayName("filtro vazio aceita tudo")
    void emptyFilterAcceptsEverything() {
        assertThat(entry(LogLevel.DEBUG, "order-service", NOW).matches(LogFilter.all())).isTrue();
    }

    @Test
    @DisplayName("nível filtra por severidade MÍNIMA: pedir WARN traz WARN e ERROR")
    void levelIsAMinimum() {
        LogFilter warnOrWorse = LogFilter.all().withMinimumLevel(LogLevel.WARN);

        assertThat(entry(LogLevel.ERROR, "app", NOW).matches(warnOrWorse)).isTrue();
        assertThat(entry(LogLevel.WARN, "app", NOW).matches(warnOrWorse)).isTrue();
        assertThat(entry(LogLevel.INFO, "app", NOW).matches(warnOrWorse)).isFalse();
        assertThat(entry(LogLevel.DEBUG, "app", NOW).matches(warnOrWorse)).isFalse();
    }

    @Test
    @DisplayName("app filtra por igualdade exata")
    void appIsExact() {
        LogFilter onlyMetrics = LogFilter.all().withApp(new ApplicationName("metrics-consumer"));

        assertThat(entry(LogLevel.WARN, "metrics-consumer", NOW).matches(onlyMetrics)).isTrue();
        assertThat(entry(LogLevel.WARN, "order-service", NOW).matches(onlyMetrics)).isFalse();
    }

    @Test
    @DisplayName("período é fechado nos dois extremos")
    void rangeIsInclusive() {
        LogFilter window = LogFilter.all()
                .withRange(new TimeRange(NOW, NOW.plusSeconds(60)));

        assertThat(entry(LogLevel.INFO, "app", NOW).matches(window)).isTrue();
        assertThat(entry(LogLevel.INFO, "app", NOW.plusSeconds(60)).matches(window)).isTrue();
        assertThat(entry(LogLevel.INFO, "app", NOW.minusSeconds(1)).matches(window)).isFalse();
    }

    @Test
    @DisplayName("critérios combinam com E, não com OU")
    void criteriaCombineWithAnd() {
        LogFilter combined = LogFilter.all()
                .withMinimumLevel(LogLevel.WARN)
                .withApp(new ApplicationName("metrics-consumer"));

        assertThat(entry(LogLevel.ERROR, "metrics-consumer", NOW).matches(combined)).isTrue();
        assertThat(entry(LogLevel.ERROR, "order-service", NOW).matches(combined)).isFalse();
        assertThat(entry(LogLevel.INFO, "metrics-consumer", NOW).matches(combined)).isFalse();
    }

    @Test
    @DisplayName("LogLevel.parse tolera caixa mas recusa lixo")
    void levelParsing() {
        assertThat(LogLevel.parse("warn")).isEqualTo(LogLevel.WARN);
        assertThat(LogLevel.parse("  ERROR  ")).isEqualTo(LogLevel.ERROR);
        assertThatThrownBy(() -> LogLevel.parse("CRITICAL")).isInstanceOf(InvalidLogException.class);
        assertThatThrownBy(() -> LogLevel.parse("")).isInstanceOf(InvalidLogException.class);
    }

    @Test
    @DisplayName("um registro sem mensagem não é um registro")
    void rejectsIncompleteEntries() {
        assertThatThrownBy(() -> new LogEntry(LogLevel.INFO, NOW, new ApplicationName("app"), "  ", Map.of()))
                .isInstanceOf(InvalidLogException.class);
        assertThatThrownBy(() -> new LogEntry(null, NOW, new ApplicationName("app"), "msg", Map.of()))
                .isInstanceOf(InvalidLogException.class);
    }

    @Test
    @DisplayName("o contexto é copiado: mutar o mapa de origem não altera o registro")
    void contextIsDefensivelyCopied() {
        Map<String, String> mutable = new java.util.HashMap<>();
        mutable.put("a", "1");

        LogEntry logEntry = new LogEntry(LogLevel.INFO, NOW, new ApplicationName("app"), "msg", mutable);
        mutable.put("b", "2");

        assertThat(logEntry.context()).containsOnlyKeys("a");
    }
}
