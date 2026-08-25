package dev.joaolaureano.trainingkafka.analytics.domain.model;

import java.time.Instant;

/**
 * Uma entrada do ledger append-only de pedidos.
 *
 * Tem identidade ({@link OrderId}) mas nenhum mutador: uma vez registrado, um
 * pedido não muda. É o que torna possível responder "quanto faturamos entre estas
 * duas datas" para QUALQUER período — {@link ProductSalesRecord} acumula totais
 * sem dimensão temporal, e deve continuar assim; dar-lhe séries temporais o
 * transformaria em duas coisas ao mesmo tempo.
 */
public record OrderRecord(
        OrderId orderId,
        CustomerId customerId,
        ProductId productId,
        Quantity quantity,
        Money amount,
        Instant placedAt
) {

    public OrderRecord {
        if (orderId == null || customerId == null || productId == null
                || quantity == null || amount == null || placedAt == null) {
            throw new InvalidValueException("todos os campos de OrderRecord são obrigatórios");
        }
    }
}
