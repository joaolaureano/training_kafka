package dev.joaolaureano.trainingkafka.analytics.domain.model;

/**
 * Aggregate root: o acumulado de vendas de UM produto.
 *
 * Invariante central: unidades vendidas e faturamento sempre mudam juntos. Isso
 * não é garantido por disciplina de quem chama — é garantido pela forma da
 * classe. Existe um único mutador, {@link #registerSale}, e ele move os três
 * campos numa operação só. Não há setter que permita somar receita sem somar
 * unidades, então esse estado inconsistente não é representável.
 *
 * Fronteira de consistência: um produto. Dois produtos jamais precisam mudar
 * atomicamente, e é por isso que cada um é seu próprio agregado.
 */
public class ProductSalesRecord {

    private final ProductId productId;
    private Units unitsSold;
    private Money revenue;
    private long orderCount;

    private ProductSalesRecord(ProductId productId, Units unitsSold, Money revenue, long orderCount) {
        this.productId = productId;
        this.unitsSold = unitsSold;
        this.revenue = revenue;
        this.orderCount = orderCount;
    }

    /** Um produto visto pela primeira vez: nada vendido ainda. */
    public static ProductSalesRecord startFor(ProductId productId) {
        if (productId == null) {
            throw new InvalidValueException("productId é obrigatório");
        }
        return new ProductSalesRecord(productId, Units.NONE, Money.ZERO, 0);
    }

    /**
     * Reconstrói o agregado a partir do estado persistido.
     *
     * Existe para os repositórios e para mais ninguém: é a única forma de um
     * ProductSalesRecord nascer já com histórico, sem reprocessar todas as vendas.
     * Note que ela não dispara evento algum — reconstituir não é um fato novo.
     */
    public static ProductSalesRecord reconstitute(ProductId productId, Units unitsSold,
                                                  Money revenue, long orderCount) {
        if (productId == null) {
            throw new InvalidValueException("productId é obrigatório");
        }
        if (unitsSold == null || revenue == null) {
            throw new InvalidValueException("estado persistido incompleto para " + productId);
        }
        if (orderCount < 0) {
            throw new InvalidValueException("orderCount não pode ser negativo");
        }
        return new ProductSalesRecord(productId, unitsSold, revenue, orderCount);
    }

    /** O único mutador. Unidades, faturamento e contagem avançam juntos ou não avançam. */
    public void registerSale(Quantity units, Money amount) {
        if (units == null || amount == null) {
            throw new InvalidValueException("units e amount são obrigatórios para registrar uma venda");
        }
        this.unitsSold = this.unitsSold.plus(units);
        this.revenue = this.revenue.plus(amount);
        this.orderCount++;
    }

    public Money averageTicket() {
        return revenue.dividedBy(orderCount);
    }

    /** Qualificação contra um patamar — não posição num ranking. */
    public boolean isTopSeller(TopSellerPolicy policy) {
        if (policy == null) {
            throw new InvalidValueException("policy é obrigatória");
        }
        return unitsSold.isAtLeast(policy.minimumUnits());
    }

    public ProductId productId() {
        return productId;
    }

    public Units unitsSold() {
        return unitsSold;
    }

    public Money revenue() {
        return revenue;
    }

    public long orderCount() {
        return orderCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ProductSalesRecord other && productId.equals(other.productId);
    }

    @Override
    public int hashCode() {
        return productId.hashCode();
    }

    @Override
    public String toString() {
        return "ProductSalesRecord[" + productId + " units=" + unitsSold
                + " revenue=" + revenue + " orders=" + orderCount + "]";
    }
}
