package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import java.math.BigDecimal;

/**
 * Contrato JSON do tópico {@code inventory-events}.
 *
 * Um record só para os três desfechos, discriminados por {@code eventType} —
 * mesma forma de {@code payment-events}. Três records exigiriam três tópicos ou
 * um cabeçalho de tipo Java na mensagem; o primeiro multiplica infraestrutura, o
 * segundo acopla os consumidores ao classpath de quem produz.
 *
 * <p>{@code customerId}, {@code amount} e {@code correlationId} não são dados de
 * estoque: viajam porque o payment-service passa a ser disparado por
 * {@code StockReserved} e precisa deles para cobrar sem consultar ninguém de volta.
 */
public record InventoryEventMessage(String eventType, String eventId, String correlationId,
                                    String orderId, String customerId, String sku,
                                    int quantity, BigDecimal amount, String reason,
                                    String occurredAt) {

    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_REJECTED = "StockRejected";
    public static final String STOCK_RELEASED = "StockReleased";
}
