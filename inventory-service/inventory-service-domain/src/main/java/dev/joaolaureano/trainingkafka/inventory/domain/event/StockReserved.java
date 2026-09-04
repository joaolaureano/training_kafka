package dev.joaolaureano.trainingkafka.inventory.domain.event;

import dev.joaolaureano.trainingkafka.inventory.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * As unidades foram separadas para este pedido.
 *
 * <p>É o gatilho da cobrança: o payment-service escuta este fato, e não mais o
 * pedido cru. É por isso que o evento carrega {@code customerId}, {@code amount} e
 * {@code correlationId}, que não são dados de estoque — numa coreografia, cada
 * elo precisa entregar ao próximo o contexto de que ele depende, ou o próximo
 * teria que consultar o serviço anterior de volta e a independência entre os
 * contextos desapareceria.
 */
public record StockReserved(String orderId, Sku sku, Quantity quantity, String customerId,
                            BigDecimal amount, String correlationId, Instant occurredAt)
        implements DomainEvent {
}
