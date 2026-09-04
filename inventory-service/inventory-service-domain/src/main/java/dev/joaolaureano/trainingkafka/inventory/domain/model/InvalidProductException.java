package dev.joaolaureano.trainingkafka.inventory.domain.model;

/** Recusa do domínio: o que se pediu não descreve um produto válido. */
public class InvalidProductException extends RuntimeException {

    public InvalidProductException(String message) {
        super(message);
    }
}
