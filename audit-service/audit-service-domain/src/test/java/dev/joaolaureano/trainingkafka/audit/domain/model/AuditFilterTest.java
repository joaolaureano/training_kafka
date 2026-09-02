package dev.joaolaureano.trainingkafka.audit.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuditFilterTest {

    private static final Instant NOW = Instant.parse("2026-08-25T12:00:00Z");

    private static AuditEvent entry(AuditLevel level, String app, Instant at) {
        return new AuditEvent(level, at, new ApplicationName(app), "order.accepted", Map.of("k", "v"));
    }

    @Test
    @DisplayName("filtro vazio aceita tudo")
    void emptyFilterAcceptsEverything() {
        assertThat(entry(AuditLevel.DEBUG, "order-service", NOW).matches(AuditFilter.all())).isTrue();
    }

    @Test
    @DisplayName("nível filtra por severidade MÍNIMA: pedir WARN traz WARN e ERROR")
    void levelIsAMinimum() {
        AuditFilter warnOrWorse = AuditFilter.all().withMinimumLevel(AuditLevel.WARN);

        assertThat(entry(AuditLevel.ERROR, "app", NOW).matches(warnOrWorse)).isTrue();
        assertThat(entry(AuditLevel.WARN, "app", NOW).matches(warnOrWorse)).isTrue();
        assertThat(entry(AuditLevel.INFO, "app", NOW).matches(warnOrWorse)).isFalse();
        assertThat(entry(AuditLevel.DEBUG, "app", NOW).matches(warnOrWorse)).isFalse();
    }

    @Test
    @DisplayName("app filtra por igualdade exata")
    void appIsExact() {
        AuditFilter onlyMetrics = AuditFilter.all().withApp(new ApplicationName("metrics-consumer"));

        assertThat(entry(AuditLevel.WARN, "metrics-consumer", NOW).matches(onlyMetrics)).isTrue();
        assertThat(entry(AuditLevel.WARN, "order-service", NOW).matches(onlyMetrics)).isFalse();
    }

    @Test
    @DisplayName("período é fechado nos dois extremos")
    void rangeIsInclusive() {
        AuditFilter window = AuditFilter.all()
                .withRange(new TimeRange(NOW, NOW.plusSeconds(60)));

        assertThat(entry(AuditLevel.INFO, "app", NOW).matches(window)).isTrue();
        assertThat(entry(AuditLevel.INFO, "app", NOW.plusSeconds(60)).matches(window)).isTrue();
        assertThat(entry(AuditLevel.INFO, "app", NOW.minusSeconds(1)).matches(window)).isFalse();
    }

    @Test
    @DisplayName("critérios combinam com E, não com OU")
    void criteriaCombineWithAnd() {
        AuditFilter combined = AuditFilter.all()
                .withMinimumLevel(AuditLevel.WARN)
                .withApp(new ApplicationName("metrics-consumer"));

        assertThat(entry(AuditLevel.ERROR, "metrics-consumer", NOW).matches(combined)).isTrue();
        assertThat(entry(AuditLevel.ERROR, "order-service", NOW).matches(combined)).isFalse();
        assertThat(entry(AuditLevel.INFO, "metrics-consumer", NOW).matches(combined)).isFalse();
    }

    @Test
    @DisplayName("AuditLevel.parse tolera caixa mas recusa lixo")
    void levelParsing() {
        assertThat(AuditLevel.parse("warn")).isEqualTo(AuditLevel.WARN);
        assertThat(AuditLevel.parse("  ERROR  ")).isEqualTo(AuditLevel.ERROR);
        assertThatThrownBy(() -> AuditLevel.parse("CRITICAL")).isInstanceOf(InvalidAuditException.class);
        assertThatThrownBy(() -> AuditLevel.parse("")).isInstanceOf(InvalidAuditException.class);
    }

    @Test
    @DisplayName("um evento sem ação não é um evento")
    void rejectsIncompleteEntries() {
        assertThatThrownBy(() -> new AuditEvent(AuditLevel.INFO, NOW, new ApplicationName("app"), "  ", Map.of()))
                .isInstanceOf(InvalidAuditException.class);
        assertThatThrownBy(() -> new AuditEvent(null, NOW, new ApplicationName("app"), "order.accepted", Map.of()))
                .isInstanceOf(InvalidAuditException.class);
    }

    @Test
    @DisplayName("o contexto é copiado: mutar o mapa de origem não altera o registro")
    void contextIsDefensivelyCopied() {
        Map<String, String> mutable = new java.util.HashMap<>();
        mutable.put("a", "1");

        AuditEvent auditEvent = new AuditEvent(AuditLevel.INFO, NOW, new ApplicationName("app"), "order.accepted", mutable);
        mutable.put("b", "2");

        assertThat(auditEvent.context()).containsOnlyKeys("a");
    }
}
