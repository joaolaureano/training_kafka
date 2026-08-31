package dev.joaolaureano.trainingkafka.analytics.adapters.web;

import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Endpoints de inspeção — é por aqui que se confere o que o consumidor acumulou. */
@RestController
@RequestMapping("/metrics")
public class MetricsController {

    private final MetricsQueryPort queries;
    private final Clock clock;

    public MetricsController(MetricsQueryPort queries, Clock clock) {
        this.queries = queries;
        this.clock = clock;
    }

    @GetMapping("/top-products")
    public List<ProductSalesView> topProducts(@RequestParam(defaultValue = "10") int limit) {
        return queries.topSellingProducts(limit).stream().map(ProductSalesView::from).toList();
    }

    /** Faturamento das últimas N horas (padrão: 24), com ticket médio derivado. */
    @GetMapping("/revenue")
    public RevenueView revenue(@RequestParam(defaultValue = "24") long hours,
                               @RequestParam(required = false) String product) {
        TimeRange range = TimeRange.lastOf(Duration.ofHours(hours), Instant.now(clock));

        RevenueWindow window = product == null || product.isBlank()
                ? queries.revenueOver(range)
                : queries.revenueOver(range, new ProductId(product));

        return RevenueView.from(window);
    }

    public record ProductSalesView(String product, long unitsSold, String revenue,
                                   long orderCount, String averageTicket) {

        static ProductSalesView from(ProductSalesRecord record) {
            return new ProductSalesView(
                    record.productId().value(),
                    record.unitsSold().value(),
                    record.revenue().toString(),
                    record.orderCount(),
                    record.averageTicket().toString());
        }
    }

    public record RevenueView(String from, String to, String total,
                              long orderCount, String averageTicket) {

        static RevenueView from(RevenueWindow window) {
            return new RevenueView(
                    window.range().start().toString(),
                    window.range().end().toString(),
                    window.total().toString(),
                    window.orderCount(),
                    window.averageTicket().toString());
        }
    }
}
