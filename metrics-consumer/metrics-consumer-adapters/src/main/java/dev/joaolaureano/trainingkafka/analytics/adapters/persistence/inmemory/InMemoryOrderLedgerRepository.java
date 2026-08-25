package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory;

import dev.joaolaureano.trainingkafka.analytics.domain.model.Money;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Implementação em memória de {@link OrderLedgerRepository}, para testes e
 * ambientes onde não vale a pena subir um banco de verdade.
 *
 * {@link OrderRecord} é um record imutável, então guardar a instância recebida
 * diretamente é seguro — não há o risco de mutação por trás das costas que existe
 * com os agregados mutáveis dos outros dois repositórios.
 */
public class InMemoryOrderLedgerRepository implements OrderLedgerRepository {

    private final List<OrderRecord> ledger = new CopyOnWriteArrayList<>();

    public InMemoryOrderLedgerRepository() {
    }

    @Override
    public void append(OrderRecord record) {
        ledger.add(record);
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range) {
        return revenueOver(range, null);
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range, ProductId productId) {
        Money total = Money.ZERO;
        long orderCount = 0;
        for (OrderRecord record : ledger) {
            if (!range.contains(record.placedAt())) {
                continue;
            }
            if (productId != null && !productId.equals(record.productId())) {
                continue;
            }
            total = total.plus(record.amount());
            orderCount++;
        }
        if (orderCount == 0) {
            return RevenueWindow.empty(range);
        }
        return new RevenueWindow(range, total, orderCount);
    }
}
