package dev.joaolaureano.trainingkafka.inventory.domain.model;

import dev.joaolaureano.trainingkafka.inventory.domain.event.StockRejected;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockReleased;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockReserved;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ReservationTest {

    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");
    private static final Sku SKU = Sku.of("TECLADO");
    private static final Quantity TWO = new Quantity(2);
    private static final BigDecimal AMOUNT = new BigDecimal("199.90");

    private static Reservation held() {
        return Reservation.held("order-1", SKU, TWO, "cust-1", AMOUNT, "corr-1", NOW);
    }

    @Test
    @DisplayName("segurar estoque registra o fato que dispara a cobrança")
    void holdingRaisesStockReserved() {
        Reservation reservation = held();

        assertThat(reservation.status()).isEqualTo(ReservationStatus.HELD);
        assertThat(reservation.pendingEvents()).singleElement()
                .isInstanceOfSatisfying(StockReserved.class, event -> {
                    assertThat(event.orderId()).isEqualTo("order-1");
                    assertThat(event.customerId()).isEqualTo("cust-1");
                    assertThat(event.amount()).isEqualByComparingTo(AMOUNT);
                    assertThat(event.correlationId()).isEqualTo("corr-1");
                });
    }

    @Test
    @DisplayName("rejeitar registra o motivo, que é o que distingue os dois casos")
    void rejectionCarriesTheReason() {
        Reservation reservation = Reservation.rejected("order-1", SKU, TWO, "cust-1", AMOUNT,
                "corr-1", RejectionReason.UNKNOWN_PRODUCT, NOW);

        assertThat(reservation.status()).isEqualTo(ReservationStatus.REJECTED);
        assertThat(reservation.pendingEvents()).singleElement()
                .isInstanceOfSatisfying(StockRejected.class, event ->
                        assertThat(event.reason()).isEqualTo(RejectionReason.UNKNOWN_PRODUCT));
    }

    @Test
    @DisplayName("liberar duas vezes devolve estoque uma vez só")
    void releaseIsIdempotent() {
        Reservation reservation = held();
        reservation.pullDomainEvents();

        reservation.release("PaymentFailed", NOW);
        reservation.release("PaymentFailed", NOW);

        assertThat(reservation.status()).isEqualTo(ReservationStatus.RELEASED);
        assertThat(reservation.pendingEvents())
                .as("a segunda liberação não pode gerar um segundo fato")
                .singleElement()
                .isInstanceOf(StockReleased.class);
    }

    @Test
    @DisplayName("liberar uma reserva rejeitada não faz nada — nunca houve o que devolver")
    void releasingARejectedReservationIsANoop() {
        Reservation reservation = Reservation.rejected("order-1", SKU, TWO, "cust-1", AMOUNT,
                "corr-1", RejectionReason.OUT_OF_STOCK, NOW);
        reservation.pullDomainEvents();

        reservation.release("PaymentFailed", NOW);

        assertThat(reservation.status()).isEqualTo(ReservationStatus.REJECTED);
        assertThat(reservation.pendingEvents()).isEmpty();
    }

    @Test
    @DisplayName("pullDomainEvents drena a lista, para o fato não ser publicado duas vezes")
    void drainsEventsOnce() {
        Reservation reservation = held();

        assertThat(reservation.pullDomainEvents()).hasSize(1);
        assertThat(reservation.pullDomainEvents()).isEmpty();
    }

    @Test
    @DisplayName("a identidade é o pedido — é isso que torna a reserva idempotente")
    void identityIsTheOrder() {
        assertThat(held()).isEqualTo(
                Reservation.held("order-1", SKU, new Quantity(9), "outro", AMOUNT, "x", NOW));
        assertThat(held()).isNotEqualTo(
                Reservation.held("order-2", SKU, TWO, "cust-1", AMOUNT, "corr-1", NOW));
    }
}
