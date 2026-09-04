package dev.joaolaureano.trainingkafka.inventory.adapters.web;

/** O contrato JSON do catálogo. */
public record ProductResponse(String sku, String name, int available) {
}
