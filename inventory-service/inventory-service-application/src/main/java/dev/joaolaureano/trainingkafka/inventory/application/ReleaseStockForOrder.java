package dev.joaolaureano.trainingkafka.inventory.application;

import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Reservation;
import dev.joaolaureano.trainingkafka.inventory.domain.port.InventoryRepository;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Caso de uso: devolver ao estoque o que foi separado para um pedido que não vai
 * acontecer.
 *
 * É a compensação da Saga do lado do estoque, e atende aos dois motivos pelos
 * quais um pedido morre depois de reservado: o pagamento falhou, ou a fraude foi
 * descoberta depois de ele já estar pago. Os dois chegam pelo mesmo tópico
 * ({@code payment-events}), porque o dono do dinheiro é quem declara que o
 * pedido acabou — o mesmo motivo pelo qual o order-service também espera por ele
 * em vez de escutar o fraud-service direto.
 */
public final class ReleaseStockForOrder {

    private static final int DEFAULT_ATTEMPTS = 5;

    private final InventoryRepository inventory;
    private final ActivityLogPublisher activityLog;
    private final Clock clock;
    private final int attempts;

    public ReleaseStockForOrder(InventoryRepository inventory, ActivityLogPublisher activityLog,
                                Clock clock) {
        this(inventory, activityLog, clock, DEFAULT_ATTEMPTS);
    }

    public ReleaseStockForOrder(InventoryRepository inventory, ActivityLogPublisher activityLog,
                                Clock clock, int attempts) {
        this.inventory = Objects.requireNonNull(inventory);
        this.activityLog = Objects.requireNonNull(activityLog);
        this.clock = Objects.requireNonNull(clock);
        this.attempts = attempts;
    }

    public void handle(String orderId, String customerId, BigDecimal amount,
                       String correlationId, String reason) {
        OptimisticRetry.run(attempts,
                () -> release(orderId, customerId, amount, correlationId, reason));
    }

    private void release(String orderId, String customerId, BigDecimal amount,
                         String correlationId, String reason) {
        Instant now = clock.instant();
        Optional<Reservation> found = inventory.findReservation(orderId);

        /*
         * Sem reserva, a compensação venceu a corrida com o próprio pedido.
         *
         * Parece impossível: o pagamento só é disparado por StockReserved, que este
         * contexto publica DEPOIS de commitar a reserva. Mas há um caminho em que não
         * há pagamento nenhum antes — a detecção de fraude cancela um pagamento que
         * ainda não existia, criando-o já cancelado. Esse PaymentCancelled pode chegar
         * aqui antes de o OrderPlaced ter sido consumido.
         *
         * Registrar e sair seria a resposta errada: o OrderPlaced chegaria em seguida,
         * reservaria estoque para um pedido já cancelado, e nada jamais o devolveria —
         * a mensagem que o devolveria já passou. A lápide fecha isso pela mesma porta
         * que a idempotência já usa.
         */
        if (found.isEmpty()) {
            Reservation voided = Reservation.voided(orderId, customerId, amount, correlationId,
                    reason, now);
            inventory.save(voided, voided.pullDomainEvents());

            activityLog.publish(ActivityLog.warn("inventory.release.voided", ActivityLog.context(
                    "orderId", orderId,
                    "reason", String.valueOf(reason),
                    "detail", "compensação chegou antes do pedido; reserva bloqueada"), now));
            return;
        }

        Reservation reservation = found.get();
        // Já liberada (reentrega) ou rejeitada (nunca segurou nada): nada a fazer.
        if (!reservation.isHeld()) {
            return;
        }

        Product product = inventory.findBySku(reservation.sku())
                .orElseThrow(() -> new IllegalStateException(
                        "reserva " + orderId + " aponta para um produto que não existe: "
                                + reservation.sku()));

        product.release(reservation.quantity());
        reservation.release(reason, now);
        inventory.save(reservation, product, reservation.pullDomainEvents());

        activityLog.publish(ActivityLog.warn("inventory.released", ActivityLog.context(
                "orderId", orderId,
                "sku", reservation.sku().value(),
                "quantity", reservation.quantity().toString(),
                "available", product.available().toString(),
                "reason", String.valueOf(reason),
                "correlationId", String.valueOf(reservation.correlationId())), now));
    }
}
