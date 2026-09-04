package dev.joaolaureano.trainingkafka.inventory.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Nenhum @SpringBootTest, nenhum contexto, nenhum mock: milissegundos para rodar.
 * O domínio não depende de nada, e é isso que torna a invariante barata de testar.
 */
class ProductTest {

    private static Product withStock(int available) {
        return Product.define("TECLADO", "Teclado mecânico", available);
    }

    @Nested
    @DisplayName("ao reservar")
    class WhenReserving {

        @Test
        @DisplayName("separa unidades e desconta do disponível")
        void reservesAndDiscounts() {
            Product product = withStock(10);

            product.reserve(new Quantity(3));

            assertThat(product.available().value()).isEqualTo(7);
        }

        @Test
        @DisplayName("recusa o que não tem — a razão de este agregado existir")
        void refusesMoreThanAvailable() {
            Product product = withStock(2);

            assertThatThrownBy(() -> product.reserve(new Quantity(3)))
                    .isInstanceOf(InsufficientStockException.class);
            assertThat(product.available().value())
                    .as("uma reserva recusada não pode ter mexido no estoque")
                    .isEqualTo(2);
        }

        @Test
        @DisplayName("a última unidade pode ser vendida; a seguinte não")
        void theLastUnitIsSellable() {
            Product product = withStock(1);

            product.reserve(new Quantity(1));
            assertThat(product.available().value()).isZero();

            assertThatThrownBy(() -> product.reserve(new Quantity(1)))
                    .isInstanceOf(InsufficientStockException.class);
        }

        @Test
        @DisplayName("reservar zero não é reserva")
        void refusesNonPositiveQuantity() {
            assertThatThrownBy(() -> withStock(10).reserve(Quantity.ZERO))
                    .isInstanceOf(InvalidProductException.class);
        }

        @Test
        @DisplayName("a exceção diz quanto foi pedido e quanto havia")
        void carriesTheNumbers() {
            Product product = withStock(2);

            assertThatThrownBy(() -> product.reserve(new Quantity(5)))
                    .isInstanceOfSatisfying(InsufficientStockException.class, failure -> {
                        assertThat(failure.requested().value()).isEqualTo(5);
                        assertThat(failure.available().value()).isEqualTo(2);
                        assertThat(failure.sku().value()).isEqualTo("TECLADO");
                    });
        }
    }

    @Nested
    @DisplayName("ao devolver")
    class WhenReleasing {

        @Test
        @DisplayName("reserva e devolução se cancelam exatamente")
        void releaseUndoesReserve() {
            Product product = withStock(10);
            Quantity three = new Quantity(3);

            product.reserve(three);
            product.release(three);

            assertThat(product.available().value()).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("ao definir o produto")
    class WhenDefining {

        @Test
        @DisplayName("estoque zero é um produto legítimo, só esgotado")
        void zeroStockIsValid() {
            assertThat(withStock(0).available().value()).isZero();
        }

        @Test
        @DisplayName("recusa nome vazio e quantidade negativa")
        void refusesInvalidInput() {
            assertThatThrownBy(() -> Product.define("SKU", "  ", 1))
                    .isInstanceOf(InvalidProductException.class);
            assertThatThrownBy(() -> Product.define("SKU", "Nome", -1))
                    .isInstanceOf(InvalidProductException.class);
            assertThatThrownBy(() -> Product.define(" ", "Nome", 1))
                    .isInstanceOf(InvalidProductException.class);
        }

        @Test
        @DisplayName("o SKU é normalizado — senão o mesmo produto teria dois estoques")
        void trimsTheSku() {
            assertThat(Product.define("  TECLADO ", "Teclado", 1).sku())
                    .isEqualTo(Sku.of("TECLADO"));
        }

        @Test
        @DisplayName("redefinir preserva a versão lida, para o bloqueio otimista continuar valendo")
        void redefinitionKeepsTheVersion() {
            Product loaded = Product.reconstitute(Sku.of("TECLADO"), "Teclado", new Quantity(5), 7L);

            Product redefined = loaded.redefinedAs("Teclado novo", 40);

            assertThat(redefined.version()).isEqualTo(7L);
            assertThat(redefined.available().value()).isEqualTo(40);
            assertThat(redefined.name()).isEqualTo("Teclado novo");
        }
    }
}
