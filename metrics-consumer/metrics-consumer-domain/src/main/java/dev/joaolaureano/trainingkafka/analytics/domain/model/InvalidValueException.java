package dev.joaolaureano.trainingkafka.analytics.domain.model;

/** Um valor recusado na fronteira do domínio de análise. */
public class InvalidValueException extends RuntimeException {

    public InvalidValueException(String message) {
        super(message);
    }
}
