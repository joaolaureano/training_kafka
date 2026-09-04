package dev.joaolaureano.trainingkafka.inventory.adapters.web;

import java.util.List;
import java.util.Optional;

public interface FindProductPort {

    Optional<ProductResponse> bySku(String sku);

    List<ProductResponse> all();
}
