package dev.joaolaureano.trainingkafka.orders.domain.model;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Sinaliza que uma tentativa de criar um pedido viola uma ou mais regras do domínio.
 *
 * Carrega a lista completa de violações, e não apenas a primeira: quem chamou
 * merece saber tudo o que está errado de uma vez só.
 */
public class InvalidOrderException extends RuntimeException {

    private final transient List<Violation> violations;

    /** Usado pelos value objects, que validam um campo só e falham na hora. */
    public InvalidOrderException(String message) {
        super(message);
        this.violations = List.of(new Violation(null, message));
    }

    /** Usado pelo agregado, que acumula tudo antes de recusar. */
    public InvalidOrderException(List<Violation> violations) {
        super(summarize(violations));
        this.violations = List.copyOf(violations);
    }

    public List<Violation> violations() {
        return violations;
    }

    private static String summarize(List<Violation> violations) {
        return violations.stream().map(Violation::toString).collect(Collectors.joining("; "));
    }
}
