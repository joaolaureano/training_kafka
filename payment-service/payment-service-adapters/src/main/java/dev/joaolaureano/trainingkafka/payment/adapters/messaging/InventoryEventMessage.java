package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import java.math.BigDecimal;

/**
 * Contrato de fio de {@code inventory-events} — o gatilho da cobrança.
 *
 * O App E deixou de escutar {@code orders} porque cobrar antes de haver estoque
 * significaria estornar depois. A ordem passou a ser: o pedido reserva, e só o
 * que foi reservado é cobrado.
 *
 * Todos os campos de que a cobrança precisa viajam aqui — {@code customerId},
 * {@code amount} e {@code correlationId} —, e é por isso que este contexto não
 * precisa consultar nem o pedido nem o estoque de volta.
 */
public record InventoryEventMessage(String eventType, String eventId, String correlationId,
                                    String orderId, String customerId, String sku,
                                    int quantity, BigDecimal amount, String reason,
                                    String occurredAt) {
}
