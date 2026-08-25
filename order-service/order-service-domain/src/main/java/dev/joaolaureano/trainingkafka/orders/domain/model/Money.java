package dev.joaolaureano.trainingkafka.orders.domain.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Valor monetário, sempre com duas casas decimais e nunca negativo.
 *
 * Existe para que nenhum {@code BigDecimal} cru — e muito menos um {@code double} —
 * circule pelo domínio carregando ambiguidade de escala ou erro de arredondamento.
 */
public record Money(BigDecimal amount) implements Comparable<Money> {

    public static final Money ZERO = new Money(BigDecimal.ZERO);

    public Money {
        if (amount == null) {
            throw new InvalidOrderException("amount é obrigatório");
        }
        if (amount.signum() < 0) {
            throw new InvalidOrderException("amount não pode ser negativo, recebido: " + amount);
        }
        amount = amount.setScale(2, RoundingMode.HALF_UP);
    }

    public static Money of(String amount) {
        if (amount == null) {
            throw new InvalidOrderException("amount é obrigatório");
        }
        return new Money(new BigDecimal(amount));
    }

    public Money plus(Money other) {
        return new Money(this.amount.add(other.amount));
    }

    public Money times(Quantity quantity) {
        return new Money(this.amount.multiply(BigDecimal.valueOf(quantity.value())));
    }

    public boolean isPositive() {
        return amount.signum() > 0;
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
