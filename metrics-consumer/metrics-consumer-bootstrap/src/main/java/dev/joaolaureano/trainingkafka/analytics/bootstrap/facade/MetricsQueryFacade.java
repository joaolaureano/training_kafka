package dev.joaolaureano.trainingkafka.analytics.bootstrap.facade;

import dev.joaolaureano.trainingkafka.analytics.adapters.web.MetricsQueryPort;
import dev.joaolaureano.trainingkafka.analytics.application.MetricsQueryService;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;

import java.util.List;
import java.util.Objects;

/** Liga os endpoints de inspeção ao lado de leitura da aplicação. */
public class MetricsQueryFacade implements MetricsQueryPort {

    private final MetricsQueryService queries;

    public MetricsQueryFacade(MetricsQueryService queries) {
        this.queries = Objects.requireNonNull(queries);
    }

    @Override
    public List<ProductSalesRecord> topSellingProducts(int limit) {
        return queries.topSellingProducts(limit);
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range) {
        return queries.revenueOver(range);
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range, ProductId productId) {
        return queries.revenueOver(range, productId);
    }
}
