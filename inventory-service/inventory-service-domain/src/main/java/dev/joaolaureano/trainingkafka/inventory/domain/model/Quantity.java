package dev.joaolaureano.trainingkafka.inventory.domain.model;

/**
 * Uma contagem de unidades.
 *
 * Zero é legítimo — um produto pode existir no catálogo com estoque esgotado —
 * mas negativo não é: nenhuma sequência de operações válidas produz estoque
 * negativo, e deixar o tipo aceitá-lo seria mover a invariante para quem usa.
 */
public record Quantity(int value) {

    public Quantity {
        if (value < 0) {
            throw new InvalidProductException("quantity não pode ser negativa");
        }
    }

    public static Quantity of(Integer value) {
        if (value == null) {
            throw new InvalidProductException("quantity é obrigatória");
        }
        return new Quantity(value);
    }

    public static final Quantity ZERO = new Quantity(0);

    public boolean isPositive() {
        return value > 0;
    }

    public boolean coversAtLeast(Quantity other) {
        return value >= other.value;
    }

    public Quantity minus(Quantity other) {
        return new Quantity(value - other.value);
    }

    public Quantity plus(Quantity other) {
        return new Quantity(value + other.value);
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }
}
