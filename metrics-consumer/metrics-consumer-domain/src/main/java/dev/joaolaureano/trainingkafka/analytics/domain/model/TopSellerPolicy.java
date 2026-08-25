package dev.joaolaureano.trainingkafka.analytics.domain.model;

/**
 * O patamar a partir do qual um produto se qualifica como top seller.
 *
 * Note que isto é qualificação, não ranking. Um agregado não enxerga além da
 * própria fronteira, então {@link ProductSalesRecord} não pode — e não deve —
 * saber sua posição relativa aos outros. "Os 5 mais vendidos" é uma consulta, e
 * mora em {@code ProductSalesRepository.topSelling}.
 */
public record TopSellerPolicy(Units minimumUnits) {

    public TopSellerPolicy {
        if (minimumUnits == null) {
            throw new InvalidValueException("minimumUnits é obrigatório");
        }
    }

    public static TopSellerPolicy above(long units) {
        return new TopSellerPolicy(new Units(units));
    }
}
