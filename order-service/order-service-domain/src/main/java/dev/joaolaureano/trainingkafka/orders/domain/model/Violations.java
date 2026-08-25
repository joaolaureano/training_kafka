package dev.joaolaureano.trainingkafka.orders.domain.model;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Acumulador de violações usado pelo agregado durante a construção.
 *
 * Concilia duas coisas que parecem brigar: value objects que falham no primeiro
 * erro (e portanto nunca existem num estado inválido) e uma mensagem de recusa
 * que lista tudo o que está errado. A conciliação é {@link #capture}: tenta
 * construir o VO, e se ele recusar, anota e segue em frente.
 */
public final class Violations {

    private final List<Violation> collected = new ArrayList<>();

    /**
     * Tenta construir um value object. Se ele recusar, registra a violação sob o
     * nome do campo e devolve {@code null} — cabe ao chamador não usar o valor.
     */
    public <T> T capture(String field, Supplier<T> construction) {
        try {
            return construction.get();
        } catch (InvalidOrderException rejection) {
            collected.add(new Violation(field, rejection.getMessage()));
            return null;
        }
    }

    public void add(String field, String message) {
        collected.add(new Violation(field, message));
    }

    public boolean isEmpty() {
        return collected.isEmpty();
    }

    public List<Violation> collected() {
        return List.copyOf(collected);
    }

    public void throwIfAny() {
        if (!collected.isEmpty()) {
            throw new InvalidOrderException(collected);
        }
    }
}
