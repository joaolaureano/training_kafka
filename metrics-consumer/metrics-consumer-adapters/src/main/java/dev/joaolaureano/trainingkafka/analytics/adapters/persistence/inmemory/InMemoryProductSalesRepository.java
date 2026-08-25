package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory;

import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementação em memória de {@link ProductSalesRepository}, para testes e
 * ambientes onde não vale a pena subir um banco de verdade.
 *
 * O mapa guarda o estado por {@code productId}; nada é gravado em disco.
 */
public class InMemoryProductSalesRepository implements ProductSalesRepository {

    private final ConcurrentHashMap<ProductId, ProductSalesRecord> store = new ConcurrentHashMap<>();

    public InMemoryProductSalesRepository() {
    }

    @Override
    public ProductSalesRecord findOrCreate(ProductId productId) {
        ProductSalesRecord stored = store.get(productId);
        if (stored == null) {
            return ProductSalesRecord.startFor(productId);
        }
        // Devolve uma cópia reconstituída, não a instância guardada: o agregado é
        // mutável, e se o chamador registrasse uma venda nesta mesma instância a
        // mudança já estaria "persistida" antes de qualquer save() — mascarando
        // bugs que só apareceriam com um banco de verdade.
        return copyOf(stored);
    }

    @Override
    public void save(ProductSalesRecord record) {
        store.put(record.productId(), copyOf(record));
    }

    @Override
    public List<ProductSalesRecord> topSelling(int limit) {
        return store.values().stream()
                .sorted(Comparator.comparing(ProductSalesRecord::unitsSold).reversed())
                .limit(limit)
                .map(InMemoryProductSalesRepository::copyOf)
                .toList();
    }

    private static ProductSalesRecord copyOf(ProductSalesRecord record) {
        return ProductSalesRecord.reconstitute(
                record.productId(), record.unitsSold(), record.revenue(), record.orderCount());
    }
}
