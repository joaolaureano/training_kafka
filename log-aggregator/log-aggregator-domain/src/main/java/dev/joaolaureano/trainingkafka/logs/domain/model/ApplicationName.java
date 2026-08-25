package dev.joaolaureano.trainingkafka.logs.domain.model;

/** Qual serviço emitiu o registro. */
public record ApplicationName(String value) {

    public ApplicationName {
        if (value == null || value.isBlank()) {
            throw new InvalidLogException("app é obrigatório");
        }
        value = value.trim();
    }

    @Override
    public String toString() {
        return value;
    }
}
