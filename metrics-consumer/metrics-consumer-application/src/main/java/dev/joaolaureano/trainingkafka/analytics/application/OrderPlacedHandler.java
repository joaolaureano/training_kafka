package dev.joaolaureano.trainingkafka.analytics.application;

import dev.joaolaureano.trainingkafka.analytics.domain.event.OrderPlaced;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerOrderPattern;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.SuspicionPolicy;
import dev.joaolaureano.trainingkafka.analytics.domain.port.CustomerPatternRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.DomainEventPublisher;
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
    private final CustomerPatternRepository customerPatterns;
    private final DomainEventPublisher eventPublisher;
    private final SuspicionPolicy suspicionPolicy;

    public OrderPlacedHandler(OrderLedgerRepository ledger,
                              ProductSalesRepository productSales,
                              CustomerPatternRepository customerPatterns,
                              DomainEventPublisher eventPublisher,
                              SuspicionPolicy suspicionPolicy) {
        this.ledger = Objects.requireNonNull(ledger);
        this.productSales = Objects.requireNonNull(productSales);
        this.customerPatterns = Objects.requireNonNull(customerPatterns);
        this.eventPublisher = Objects.requireNonNull(eventPublisher);
        this.suspicionPolicy = Objects.requireNonNull(suspicionPolicy);
    }

    public void handle(OrderPlaced event) {
        appendToLedger(event);
        accumulateProductSales(event);
        evaluateCustomerPattern(event);
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

    private void evaluateCustomerPattern(OrderPlaced event) {
        CustomerOrderPattern pattern =
                customerPatterns.findOrCreate(event.customerId(), suspicionPolicy);

        pattern.registerOrder(event.orderId(), event.occurredAt(), event.amount());
        customerPatterns.save(pattern);

        // Se o agregado concluiu que há algo estranho, ele já registrou o fato.
        // Aqui só se despacha o que ele decidiu — sem reavaliar nada.
        pattern.pullDomainEvents().forEach(eventPublisher::publish);
    }
}
