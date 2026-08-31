package dev.joaolaureano.trainingkafka.analytics.adapters.web;

import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;

import java.util.List;

/**
 * O que o adapter web precisa para responder os endpoints de inspeção.
 *
 * Declarada do lado do consumidor: o controller compila conhecendo só o domínio,
 * e quem atende — o caso de uso de leitura, via facade — é decisão do bootstrap.
 */
public interface MetricsQueryPort {

    List<ProductSalesRecord> topSellingProducts(int limit);

    RevenueWindow revenueOver(TimeRange range);

    RevenueWindow revenueOver(TimeRange range, ProductId productId);
}
