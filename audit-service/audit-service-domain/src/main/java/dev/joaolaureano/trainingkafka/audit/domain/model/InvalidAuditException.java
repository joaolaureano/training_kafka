package dev.joaolaureano.trainingkafka.audit.domain.model;

/** Um registro de log recusado na fronteira do domínio. */
public class InvalidAuditException extends RuntimeException {

    public InvalidAuditException(String message) {
        super(message);
    }
}
