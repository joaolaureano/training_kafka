package dev.joaolaureano.trainingkafka.inventory.domain.event;

import dev.joaolaureano.trainingkafka.inventory.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.inventory.domain.model.RejectionReason;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Não há o que separar: o produto não existe, ou não há unidades.
 *
 * O pedido termina aqui, e termina sem nunca ter sido cobrado. Esse é o ganho de
 * reservar antes de cobrar: não existe estorno a fazer porque não houve cobrança.
 */
public record StockRejected(String orderId, Sku sku, Quantity quantity, String customerId,
                            BigDecimal amount, String correlationId, RejectionReason reason,
                            Instant occurredAt) implements DomainEvent {
}
