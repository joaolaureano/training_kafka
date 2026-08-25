package dev.joaolaureano.trainingkafka.analytics.adapters.persistence;

import dev.joaolaureano.trainingkafka.analytics.domain.model.Money;

import java.math.BigDecimal;

/**
 * Converte {@link Money} de e para centavos inteiros, para persistência.
 *
 * Por que centavos e não DECIMAL ou TEXT: SQLite não tem tipo decimal de verdade
 * (armazenaria como REAL, com erro de ponto flutuante) e TEXT não permitiria
 * {@code SUM()} nas consultas de faturamento. Um BIGINT de centavos é exato,
 * somável, e se comporta igual nos dois bancos.
 *
 * Vive na camada de adapters porque é uma decisão de armazenamento — o domínio
 * não sabe nem precisa saber como o dinheiro dele é guardado.
 */
public final class MoneyCents {

    private MoneyCents() {
    }

    public static long toCents(Money money) {
        return money.amount().movePointRight(2).longValueExact();
    }

    public static Money fromCents(long cents) {
        return new Money(BigDecimal.valueOf(cents, 2));
    }
}
