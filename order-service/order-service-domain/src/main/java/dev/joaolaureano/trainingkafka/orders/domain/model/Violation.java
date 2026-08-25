package dev.joaolaureano.trainingkafka.orders.domain.model;

/**
 * Uma invariante violada, com o campo que a causou.
 *
 * O campo é apenas um rótulo para quem for reportar o erro — o domínio não sabe
 * que existe um formulário HTTP do outro lado, só nomeia o que recusou.
 */
public record Violation(String field, String message) {

    @Override
    public String toString() {
        return field == null ? message : field + ": " + message;
    }
}
