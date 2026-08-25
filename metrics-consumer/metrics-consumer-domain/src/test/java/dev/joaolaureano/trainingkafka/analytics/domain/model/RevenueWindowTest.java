package dev.joaolaureano.trainingkafka.analytics.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RevenueWindowTest {

    private static final Instant NOW = Instant.parse("2026-08-25T12:00:00Z");
    private static final TimeRange LAST_HOUR = TimeRange.lastOf(Duration.ofHours(1), NOW);

    @Test
    @DisplayName("ticket médio deriva do total e da contagem")
    void averageTicket() {
        RevenueWindow window = new RevenueWindow(LAST_HOUR, Money.of("1000.00"), 8);

        assertThat(window.averageTicket().toString()).isEqualTo("125.00");
    }

    @Test
    @DisplayName("período sem pedidos tem ticket médio zero")
    void emptyWindow() {
        RevenueWindow window = RevenueWindow.empty(LAST_HOUR);

        assertThat(window.total()).isEqualTo(Money.ZERO);
        assertThat(window.orderCount()).isZero();
        assertThat(window.averageTicket()).isEqualTo(Money.ZERO);
    }

    @Test
    @DisplayName("intervalo invertido é recusado na construção")
    void rejectsInvertedRange() {
        assertThatThrownBy(() -> new TimeRange(NOW, NOW.minusSeconds(1)))
                .isInstanceOf(InvalidValueException.class);
    }

    @Test
    @DisplayName("igualdade por valor: é um value object, não uma entidade")
    void equalityIsByValue() {
        assertThat(new RevenueWindow(LAST_HOUR, Money.of("10.00"), 1))
                .isEqualTo(new RevenueWindow(LAST_HOUR, Money.of("10.00"), 1));
    }
}
