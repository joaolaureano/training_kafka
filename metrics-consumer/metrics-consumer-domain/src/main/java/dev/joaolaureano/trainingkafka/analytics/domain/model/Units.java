package dev.joaolaureano.trainingkafka.analytics.domain.model;

/** Unidades acumuladas de um produto. Nunca negativa, mas legitimamente zero. */
public record Units(long value) implements Comparable<Units> {

    public static final Units NONE = new Units(0);

    public Units {
        if (value < 0) {
            throw new InvalidValueException("units não pode ser negativo, recebido: " + value);
        }
    }

    public Units plus(Quantity quantity) {
        return new Units(this.value + quantity.value());
    }

    public boolean isAtLeast(Units other) {
        return this.value >= other.value;
    }

    @Override
    public int compareTo(Units other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }
}
