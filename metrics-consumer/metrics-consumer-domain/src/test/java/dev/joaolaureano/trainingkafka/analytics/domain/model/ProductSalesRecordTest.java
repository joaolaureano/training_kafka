package dev.joaolaureano.trainingkafka.analytics.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProductSalesRecordTest {

    private static final ProductId TECLADO = new ProductId("Teclado");

    @Test
    @DisplayName("um produto novo começa zerado, e isso é um estado válido")
    void startsEmpty() {
        ProductSalesRecord record = ProductSalesRecord.startFor(TECLADO);

        assertThat(record.unitsSold()).isEqualTo(Units.NONE);
        assertThat(record.revenue()).isEqualTo(Money.ZERO);
        assertThat(record.orderCount()).isZero();
    }

    @Test
    @DisplayName("unidades e faturamento avançam sempre juntos")
    void unitsAndRevenueMoveTogether() {
        ProductSalesRecord record = ProductSalesRecord.startFor(TECLADO);

        record.registerSale(new Quantity(2), Money.of("100.00"));
        record.registerSale(new Quantity(3), Money.of("150.00"));

        assertThat(record.unitsSold().value()).isEqualTo(5);
        assertThat(record.revenue().toString()).isEqualTo("250.00");
        assertThat(record.orderCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("não existe caminho para somar receita sem somar unidades")
    void invariantIsStructural() {
        // A prova é a ausência de API: a classe expõe UM mutador, e ele move os
        // três campos. Este teste documenta a intenção — se alguém acrescentar um
        // setter no futuro, a revisão tem onde se apoiar.
        assertThat(ProductSalesRecord.class.getMethods())
                .filteredOn(method -> method.getName().startsWith("set"))
                .isEmpty();
    }

    @Test
    @DisplayName("ticket médio sem vendas é zero, não divisão por zero")
    void averageTicketOnEmpty() {
        assertThat(ProductSalesRecord.startFor(TECLADO).averageTicket()).isEqualTo(Money.ZERO);
    }

    @Test
    @DisplayName("ticket médio é faturamento sobre número de pedidos")
    void averageTicket() {
        ProductSalesRecord record = ProductSalesRecord.startFor(TECLADO);
        record.registerSale(new Quantity(1), Money.of("100.00"));
        record.registerSale(new Quantity(1), Money.of("50.00"));

        assertThat(record.averageTicket().toString()).isEqualTo("75.00");
    }

    @Test
    @DisplayName("top seller é qualificação contra um patamar, não posição em ranking")
    void topSellerIsQualification() {
        ProductSalesRecord record = ProductSalesRecord.startFor(TECLADO);
        record.registerSale(new Quantity(10), Money.of("500.00"));

        assertThat(record.isTopSeller(TopSellerPolicy.above(10))).isTrue();
        assertThat(record.isTopSeller(TopSellerPolicy.above(11))).isFalse();
    }

    @Test
    @DisplayName("reconstituição preserva o acumulado sem reprocessar vendas")
    void reconstitutes() {
        ProductSalesRecord restored = ProductSalesRecord.reconstitute(
                TECLADO, new Units(42), Money.of("999.90"), 7);

        assertThat(restored.unitsSold().value()).isEqualTo(42);
        assertThat(restored.revenue().toString()).isEqualTo("999.90");
        assertThat(restored.orderCount()).isEqualTo(7);
    }

    @Test
    @DisplayName("recusa estado persistido incoerente")
    void rejectsBrokenState() {
        assertThatThrownBy(() -> ProductSalesRecord.reconstitute(TECLADO, null, Money.ZERO, 0))
                .isInstanceOf(InvalidValueException.class);
        assertThatThrownBy(() -> ProductSalesRecord.reconstitute(TECLADO, Units.NONE, Money.ZERO, -1))
                .isInstanceOf(InvalidValueException.class);
    }

    @Test
    @DisplayName("identidade é o produto: dois registros do mesmo produto são o mesmo agregado")
    void identityIsProductId() {
        assertThat(ProductSalesRecord.startFor(TECLADO))
                .isEqualTo(ProductSalesRecord.reconstitute(TECLADO, new Units(99), Money.of("1.00"), 3));
    }
}
