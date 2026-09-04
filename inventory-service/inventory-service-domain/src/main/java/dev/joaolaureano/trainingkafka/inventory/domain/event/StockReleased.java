package dev.joaolaureano.trainingkafka.inventory.domain.event;

import dev.joaolaureano.trainingkafka.inventory.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * As unidades voltaram para o estoque.
 *
 * A compensação da Saga do lado do estoque: o pagamento falhou, ou a fraude foi
 * descoberta depois, e o que estava separado para este pedido volta a poder ser
 * vendido.
 */
public record StockReleased(String orderId, Sku sku, Quantity quantity, String customerId,
                            BigDecimal amount, String correlationId, String reason,
                            Instant occurredAt) implements DomainEvent {
}
