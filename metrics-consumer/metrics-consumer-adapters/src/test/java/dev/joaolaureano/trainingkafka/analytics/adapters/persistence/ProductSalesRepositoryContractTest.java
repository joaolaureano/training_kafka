package dev.joaolaureano.trainingkafka.analytics.adapters.persistence;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb.DuckDbProductSalesRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory.InMemoryProductSalesRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite.SqliteProductSalesRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.model.*;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A MESMA bateria, contra as três implementações.
 *
 * Este arquivo é o teste central do exercício. Se as três passam nas mesmas
 * asserções escritas puramente em vocabulário de domínio, então o Port é de fato
 * uma abstração — e não um contrato moldado por acidente em torno de uma
 * tecnologia específica. Um teste que precisasse de um `if (isSqlite)` seria a
 * prova de que o desenho vazou.
 */
class ProductSalesRepositoryContractTest {

    private static final ProductId TECLADO = new ProductId("Teclado");
    private static final ProductId MOUSE = new ProductId("Mouse");

    static Stream<Arguments> implementations() {
        return Stream.of(
                Arguments.of("inmemory", (Supplier<ProductSalesRepository>)
                        InMemoryProductSalesRepository::new),
                Arguments.of("sqlite", (Supplier<ProductSalesRepository>)
                        () -> new SqliteProductSalesRepository(TestRepositories.freshSqlite())),
                Arguments.of("duckdb", (Supplier<ProductSalesRepository>)
                        () -> new DuckDbProductSalesRepository(TestRepositories.freshDuckDb())));
    }

    @ParameterizedTest(name = "[{0}] produto desconhecido volta zerado, nunca null")
    @MethodSource("implementations")
    void findOrCreateReturnsZeroed(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        ProductSalesRecord record = repository.findOrCreate(TECLADO);

        assertThat(record).isNotNull();
        assertThat(record.productId()).isEqualTo(TECLADO);
        assertThat(record.unitsSold()).isEqualTo(Units.NONE);
        assertThat(record.revenue()).isEqualTo(Money.ZERO);
        assertThat(record.orderCount()).isZero();
    }

    @ParameterizedTest(name = "[{0}] findOrCreate não grava nada sozinho")
    @MethodSource("implementations")
    void findOrCreateDoesNotPersist(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        repository.findOrCreate(TECLADO);

        assertThat(repository.topSelling(10)).isEmpty();
    }

    @ParameterizedTest(name = "[{0}] o que foi salvo é o que volta")
    @MethodSource("implementations")
    void savesAndReloads(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        ProductSalesRecord record = repository.findOrCreate(TECLADO);
        record.registerSale(new Quantity(3), Money.of("299.97"));
        repository.save(record);

        ProductSalesRecord reloaded = repository.findOrCreate(TECLADO);
        assertThat(reloaded.unitsSold().value()).isEqualTo(3);
        assertThat(reloaded.revenue().toString()).isEqualTo("299.97");
        assertThat(reloaded.orderCount()).isEqualTo(1);
    }

    @ParameterizedTest(name = "[{0}] salvar duas vezes acumula, não duplica linha")
    @MethodSource("implementations")
    void saveIsUpsert(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        for (int i = 0; i < 3; i++) {
            ProductSalesRecord record = repository.findOrCreate(TECLADO);
            record.registerSale(new Quantity(2), Money.of("100.00"));
            repository.save(record);
        }

        assertThat(repository.topSelling(10)).hasSize(1);
        ProductSalesRecord reloaded = repository.findOrCreate(TECLADO);
        assertThat(reloaded.unitsSold().value()).isEqualTo(6);
        assertThat(reloaded.revenue().toString()).isEqualTo("300.00");
        assertThat(reloaded.orderCount()).isEqualTo(3);
    }

    @ParameterizedTest(name = "[{0}] mutação sem save não vaza para o repositório")
    @MethodSource("implementations")
    void mutationWithoutSaveIsInvisible(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        ProductSalesRecord persisted = repository.findOrCreate(TECLADO);
        persisted.registerSale(new Quantity(1), Money.of("10.00"));
        repository.save(persisted);

        // Alguém pega o agregado, mexe e NÃO salva.
        ProductSalesRecord loaded = repository.findOrCreate(TECLADO);
        loaded.registerSale(new Quantity(999), Money.of("99999.00"));

        assertThat(repository.findOrCreate(TECLADO).unitsSold().value()).isEqualTo(1);
    }

    @ParameterizedTest(name = "[{0}] topSelling ordena por unidades, decrescente")
    @MethodSource("implementations")
    void topSellingIsOrdered(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        ProductSalesRecord teclado = repository.findOrCreate(TECLADO);
        teclado.registerSale(new Quantity(5), Money.of("500.00"));
        repository.save(teclado);

        ProductSalesRecord mouse = repository.findOrCreate(MOUSE);
        mouse.registerSale(new Quantity(50), Money.of("2500.00"));
        repository.save(mouse);

        assertThat(repository.topSelling(10))
                .extracting(record -> record.productId().value())
                .containsExactly("Mouse", "Teclado");
    }

    @ParameterizedTest(name = "[{0}] topSelling respeita o limite")
    @MethodSource("implementations")
    void topSellingRespectsLimit(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        for (int i = 0; i < 7; i++) {
            ProductSalesRecord record = repository.findOrCreate(new ProductId("Produto-" + i));
            record.registerSale(new Quantity(i + 1), Money.of("10.00"));
            repository.save(record);
        }

        assertThat(repository.topSelling(3)).hasSize(3);
    }

    @ParameterizedTest(name = "[{0}] centavos sobrevivem à ida e volta, sem erro de ponto flutuante")
    @MethodSource("implementations")
    void moneyRoundTripsExactly(String name, Supplier<ProductSalesRepository> factory) {
        ProductSalesRepository repository = factory.get();

        ProductSalesRecord record = repository.findOrCreate(TECLADO);
        for (int i = 0; i < 3; i++) {
            record.registerSale(new Quantity(1), Money.of("0.10"));
        }
        repository.save(record);

        // 0.10 três vezes: com double daria 0.30000000000000004.
        assertThat(repository.findOrCreate(TECLADO).revenue().toString()).isEqualTo("0.30");
    }
}
