package dev.joaolaureano.trainingkafka.analytics.domain.port;

import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;

/**
 * Port de saída do ledger append-only de pedidos.
 *
 * É a fonte das consultas de faturamento por período arbitrário — informação que
 * os totais acumulados de {@code ProductSalesRecord} não conseguem responder,
 * porque não têm dimensão temporal.
 */
public interface OrderLedgerRepository {

    void append(OrderRecord record);

    /** Faturamento e contagem de pedidos no período. Sem pedidos, devolve zerado. */
    RevenueWindow revenueOver(TimeRange range);

    /** O mesmo, restrito a um produto. */
    RevenueWindow revenueOver(TimeRange range, ProductId productId);
}
