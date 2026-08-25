package dev.joaolaureano.trainingkafka.analytics.application;

import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;

import java.util.List;
import java.util.Objects;

/** Lado de leitura: atende as consultas de inspeção expostas por HTTP. */
public class MetricsQueryService {

    private final ProductSalesRepository productSales;
    private final OrderLedgerRepository ledger;

    public MetricsQueryService(ProductSalesRepository productSales, OrderLedgerRepository ledger) {
        this.productSales = Objects.requireNonNull(productSales);
        this.ledger = Objects.requireNonNull(ledger);
    }

    public List<ProductSalesRecord> topSellingProducts(int limit) {
        return productSales.topSelling(limit);
    }

    public RevenueWindow revenueOver(TimeRange range) {
        return ledger.revenueOver(range);
    }

    public RevenueWindow revenueOver(TimeRange range, ProductId productId) {
        return ledger.revenueOver(range, productId);
    }
}
