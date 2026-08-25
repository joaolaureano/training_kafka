package dev.joaolaureano.trainingkafka.analytics.domain.model;

/** Identifica um cliente dentro do contexto de análise. */
public record CustomerId(String value) {

    public CustomerId {
        if (value == null || value.isBlank()) {
            throw new InvalidValueException("lCustomerId é obrigatório");
        }
        value = value.trim();
    }

    @Override
    public String toString() {
        return value;
    }
}
