package dev.joaolaureano.trainingkafka.inventory.bootstrap.facade;

import dev.joaolaureano.trainingkafka.inventory.adapters.web.FindProductPort;
import dev.joaolaureano.trainingkafka.inventory.adapters.web.ProductResponse;
import dev.joaolaureano.trainingkafka.inventory.adapters.web.UpsertProductPort;
import dev.joaolaureano.trainingkafka.inventory.adapters.web.UpsertProductRequest;
import dev.joaolaureano.trainingkafka.inventory.application.ManageCatalog;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * O lado HTTP da mesma ideia: os dois Ports que o controller declara encontram
 * aqui o caso de uso que os atende, e a tradução do agregado para o contrato JSON
 * acontece neste ponto — nem o domínio nem o controller precisam conhecer o outro.
 */
public class CatalogFacade implements UpsertProductPort, FindProductPort {

    private final ManageCatalog catalog;

    public CatalogFacade(ManageCatalog catalog) {
        this.catalog = Objects.requireNonNull(catalog);
    }

    @Override
    public ProductResponse upsert(String sku, UpsertProductRequest request) {
        return toResponse(catalog.upsert(sku, request.name(), request.available()));
    }

    @Override
    public Optional<ProductResponse> bySku(String sku) {
        return catalog.bySku(sku).map(CatalogFacade::toResponse);
    }

    @Override
    public List<ProductResponse> all() {
        return catalog.all().stream().map(CatalogFacade::toResponse).toList();
    }

    private static ProductResponse toResponse(Product product) {
        return new ProductResponse(product.sku().value(), product.name(),
                product.available().value());
    }
}
