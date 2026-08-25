package dev.joaolaureano.trainingkafka.analytics.domain.model;

/** Identifica um produto dentro do contexto de análise. */
public record ProductId(String value) {

    public ProductId {
        if (value == null || value.isBlank()) {
            throw new InvalidValueException("lProductId é obrigatório");
        }
        value = value.trim();
    }

    @Override
    public String toString() {
        return value;
    }
}
