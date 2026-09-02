package dev.joaolaureano.trainingkafka.audit.domain.model;

/** Qual serviço emitiu o registro. */
public record ApplicationName(String value) {

    public ApplicationName {
        if (value == null || value.isBlank()) {
            throw new InvalidAuditException("app é obrigatório");
        }
        value = value.trim();
    }

    @Override
    public String toString() {
        return value;
    }
}
