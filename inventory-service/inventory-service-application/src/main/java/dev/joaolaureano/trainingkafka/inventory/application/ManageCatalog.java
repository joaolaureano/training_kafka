package dev.joaolaureano.trainingkafka.inventory.application;

import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;
import dev.joaolaureano.trainingkafka.inventory.domain.port.InventoryRepository;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Casos de uso do catálogo: cadastrar, ajustar e consultar produtos.
 *
 * É o lado administrativo do serviço, e não participa da Saga — por isso não
 * grava evento nenhum: mudar o catálogo não é um fato que os outros contextos
 * precisem saber. Quem descobre que um produto acabou é quem tenta reservá-lo.
 */
public final class ManageCatalog {

    private final InventoryRepository inventory;
    private final ActivityLogPublisher activityLog;
    private final Clock clock;

    public ManageCatalog(InventoryRepository inventory, ActivityLogPublisher activityLog, Clock clock) {
        this.inventory = Objects.requireNonNull(inventory);
        this.activityLog = Objects.requireNonNull(activityLog);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Cria o produto ou redefine o que já existe.
     *
     * A quantidade é absoluta — "este produto tem 40 unidades" —, o que torna a
     * operação repetível sem efeito acumulado. É o que um seed de carga e um
     * ajuste de inventário precisam; reposição incremental seria outro caso de uso.
     */
    public Product upsert(String sku, String name, Integer available) {
        Product incoming = Product.define(sku, name, available);
        Product product = inventory.findBySku(incoming.sku())
                .map(existing -> existing.redefinedAs(name, available))
                .orElse(incoming);

        inventory.saveProduct(product);

        activityLog.publish(ActivityLog.info("inventory.product.upserted", ActivityLog.context(
                "sku", product.sku().value(),
                "name", product.name(),
                "available", product.available().toString()), clock.instant()));
        return product;
    }

    public Optional<Product> bySku(String sku) {
        return inventory.findBySku(Sku.of(sku));
    }

    public List<Product> all() {
        return inventory.findAll();
    }
}
