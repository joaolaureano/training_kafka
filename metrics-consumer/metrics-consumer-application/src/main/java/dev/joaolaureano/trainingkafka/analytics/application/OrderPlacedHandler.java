package dev.joaolaureano.trainingkafka.analytics.application;

import dev.joaolaureano.trainingkafka.analytics.domain.event.OrderPlaced;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;

import java.util.Objects;

/**
 * Orquestrador do evento OrderPlaced.
 *
 * Leia o método {@link #handle} e note o que NÃO tem lá: nenhum {@code if} de
 * negócio, nenhuma comparação, nenhum cálculo. Só busca agregado, chama método de
 * domínio, persiste, despacha eventos.
 *
 * Isso é o critério de qualidade do modelo, não um detalhe estético. Se um dia
 * uma regra precisar aparecer aqui — "se o cliente fez mais de N pedidos..." —
 * é porque ela pertencia a um agregado e vazou. Nesse momento, o conserto é
 * mover a regra para dentro do agregado, não escrever o {@code if}.
 */
public class OrderPlacedHandler {

    private final OrderLedgerRepository ledger;
    private final ProductSalesRepository productSales;

    public OrderPlacedHandler(OrderLedgerRepository ledger,
                              ProductSalesRepository productSales) {
        this.ledger = Objects.requireNonNull(ledger);
        this.productSales = Objects.requireNonNull(productSales);
    }

    public void handle(OrderPlaced event) {
        appendToLedger(event);
        accumulateProductSales(event);
    }

    /** O fato bruto, que sustenta as consultas de faturamento por período. */
    private void appendToLedger(OrderPlaced event) {
        ledger.append(new OrderRecord(
                event.orderId(), event.customerId(), event.productId(),
                event.quantity(), event.amount(), event.occurredAt()));
    }

    private void accumulateProductSales(OrderPlaced event) {
        ProductSalesRecord record = productSales.findOrCreate(event.productId());
        record.registerSale(event.quantity(), event.amount());
        productSales.save(record);
    }

}
