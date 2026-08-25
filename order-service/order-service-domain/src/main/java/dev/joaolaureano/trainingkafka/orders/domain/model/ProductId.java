package dev.joaolaureano.trainingkafka.orders.domain.model;

/** Identifica o produto pedido. */
public record ProductId(String value) {

    public ProductId {
        if (value == null || value.isBlank()) {
            throw new InvalidOrderException("product é obrigatório");
        }
        value = value.trim();
    }

    @Override
    public String toString() {
        return value;
    }
}
