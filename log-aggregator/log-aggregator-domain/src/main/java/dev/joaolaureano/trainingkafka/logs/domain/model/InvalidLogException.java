package dev.joaolaureano.trainingkafka.logs.domain.model;

/** Um registro de log recusado na fronteira do domínio. */
public class InvalidLogException extends RuntimeException {

    public InvalidLogException(String message) {
        super(message);
    }
}
