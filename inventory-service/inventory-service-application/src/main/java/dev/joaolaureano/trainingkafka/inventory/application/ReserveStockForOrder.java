package dev.joaolaureano.trainingkafka.inventory.application;

import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.inventory.domain.model.InsufficientStockException;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.inventory.domain.model.RejectionReason;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Reservation;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;
import dev.joaolaureano.trainingkafka.inventory.domain.port.InventoryRepository;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Caso de uso: separar estoque para um pedido que acabou de ser feito.
 *
 * É o primeiro elo da Saga depois do pedido, e o que decide se vai existir
 * cobrança. Reservar antes de cobrar é o que torna a rejeição barata: um pedido
 * sem estoque termina cancelado sem que um centavo tenha se movido, e portanto
 * sem estorno a fazer.
 *
 * Nenhum {@code if} aqui decide se há estoque — quem decide é
 * {@link Product#reserve}. Esta classe traduz, coordena, e trata as duas
 * realidades que o agregado sozinho não enxerga: a mensagem repetida e o
 * concorrente.
 */
public final class ReserveStockForOrder {

    private static final int DEFAULT_ATTEMPTS = 5;

    private final InventoryRepository inventory;
    private final ActivityLogPublisher activityLog;
    private final Clock clock;
    private final int attempts;

    public ReserveStockForOrder(InventoryRepository inventory, ActivityLogPublisher activityLog,
                                Clock clock) {
        this(inventory, activityLog, clock, DEFAULT_ATTEMPTS);
    }

    public ReserveStockForOrder(InventoryRepository inventory, ActivityLogPublisher activityLog,
                                Clock clock, int attempts) {
        this.inventory = Objects.requireNonNull(inventory);
        this.activityLog = Objects.requireNonNull(activityLog);
        this.clock = Objects.requireNonNull(clock);
        this.attempts = attempts;
    }

    public void handle(String orderId, String customerId, String product, Integer quantity,
                       BigDecimal amount, String correlationId) {
        /*
         * Idempotência antes de tudo.
         *
         * O consumo é at-least-once, e um rebalanceamento reentrega o que já foi
         * processado. Sem esta guarda, a segunda entrega descontaria o estoque de
         * novo — e o pedido receberia um segundo StockReserved, cobrando duas vezes.
         */
        if (inventory.findReservation(orderId).isPresent()) {
            return;
        }

        Sku sku = Sku.of(product);
        Quantity requested = Quantity.of(quantity);

        OptimisticRetry.run(attempts, () ->
                decide(orderId, customerId, sku, requested, amount, correlationId));
    }

    /**
     * Uma tentativa completa: lê, decide e grava.
     *
     * Tudo é relido a cada passagem de propósito. Se a gravação perder para um
     * concorrente, o produto em memória está velho — repetir a decisão em cima
     * dele daria a mesma resposta errada.
     */
    private void decide(String orderId, String customerId, Sku sku, Quantity requested,
                        BigDecimal amount, String correlationId) {
        Instant now = clock.instant();
        Optional<Product> found = inventory.findBySku(sku);

        if (found.isEmpty()) {
            reject(orderId, customerId, sku, requested, amount, correlationId,
                    RejectionReason.UNKNOWN_PRODUCT, now);
            return;
        }

        Product product = found.get();
        try {
            product.reserve(requested);
        } catch (InsufficientStockException noStock) {
            reject(orderId, customerId, sku, requested, amount, correlationId,
                    RejectionReason.OUT_OF_STOCK, now);
            return;
        }

        Reservation reservation = Reservation.held(orderId, sku, requested, customerId,
                amount, correlationId, now);
        inventory.save(reservation, product, reservation.pullDomainEvents());

        activityLog.publish(ActivityLog.info("inventory.reserved", ActivityLog.context(
                "orderId", orderId,
                "sku", sku.value(),
                "quantity", requested.toString(),
                "remaining", product.available().toString(),
                "correlationId", String.valueOf(correlationId)), now));
    }

    /**
     * A rejeição também é gravada, e não apenas publicada.
     *
     * Guardar a reserva rejeitada é o que faz a idempotência valer para os dois
     * desfechos: sem ela, uma reentrega de um pedido já rejeitado seria decidida
     * de novo — e poderia agora encontrar estoque, contradizendo o cancelamento
     * que o pedido já recebeu.
     */
    private void reject(String orderId, String customerId, Sku sku, Quantity requested,
                        BigDecimal amount, String correlationId, RejectionReason reason,
                        Instant now) {
        Reservation reservation = Reservation.rejected(orderId, sku, requested, customerId,
                amount, correlationId, reason, now);
        inventory.save(reservation, reservation.pullDomainEvents());

        activityLog.publish(ActivityLog.warn("inventory.rejected", ActivityLog.context(
                "orderId", orderId,
                "sku", sku.value(),
                "quantity", requested.toString(),
                "reason", reason.name(),
                "correlationId", String.valueOf(correlationId)), now));
    }
}
