package dev.joaolaureano.trainingkafka.analytics.domain.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Valor monetário com duas casas decimais, nunca negativo. */
public record Money(BigDecimal amount) implements Comparable<Money> {

    public static final Money ZERO = new Money(BigDecimal.ZERO);

    public Money {
        if (amount == null) {
            throw new InvalidValueException("amount é obrigatório");
        }
        if (amount.signum() < 0) {
            throw new InvalidValueException("amount não pode ser negativo, recebido: " + amount);
        }
        amount = amount.setScale(2, RoundingMode.HALF_UP);
    }

    public static Money of(String amount) {
        if (amount == null) {
            throw new InvalidValueException("amount é obrigatório");
        }
        try {
            return new Money(new BigDecimal(amount));
        } catch (NumberFormatException malformed) {
            throw new InvalidValueException("amount não é um número válido: " + amount);
        }
    }

    public Money plus(Money other) {
        return new Money(this.amount.add(other.amount));
    }

    /** Divisão para calcular médias. Por zero devolve zero — não existe ticket médio de nada. */
    public Money dividedBy(long divisor) {
        if (divisor <= 0) {
            return ZERO;
        }
        return new Money(this.amount.divide(BigDecimal.valueOf(divisor), 2, RoundingMode.HALF_UP));
    }

    @Override
    public int compareTo(Money other) {
        return this.amount.compareTo(other.amount);
    }

    @Override
    public String toString() {
        return amount.toPlainString();
    }
}
