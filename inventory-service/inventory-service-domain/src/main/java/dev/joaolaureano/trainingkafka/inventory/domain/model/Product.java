package dev.joaolaureano.trainingkafka.inventory.domain.model;

/**
 * Aggregate root do catálogo: um produto e o quanto dele existe para vender.
 *
 * A invariante que este agregado protege é uma só, e é a razão de ele existir:
 * {@code available} nunca fica negativo. Nenhum caso de uso tem permissão de
 * subtrair estoque por conta própria — tem que passar por {@link #reserve}.
 *
 * <p><b>Sobre a {@code version}:</b> ela é o número da versão com que este objeto
 * foi lido do banco, e serve ao bloqueio otimista. Dois pedidos concorrentes pela
 * última unidade leem o mesmo estado e ambos passam por {@code reserve} — o
 * agregado, sozinho, não tem como saber do outro. Quem desempata é a gravação
 * condicionada a esta versão: a segunda encontra a linha já alterada e falha, e o
 * caso de uso relê e decide de novo, agora com o estoque real. É a única forma de
 * a invariante sobreviver a mais de uma thread; guardá-la só na memória do
 * agregado seria protegê-la apenas contra um mundo de um consumidor só.
 */
public class Product {

    private final Sku sku;
    private String name;
    private Quantity available;
    private final long version;

    private Product(Sku sku, String name, Quantity available, long version) {
        this.sku = sku;
        this.name = name;
        this.available = available;
        this.version = version;
    }

    /**
     * Cria ou redefine um produto do catálogo, a partir de valores crus vindos do
     * HTTP. Única porta de entrada, e portanto único lugar onde a validação mora.
     */
    public static Product define(String sku, String name, Integer available) {
        if (name == null || name.isBlank()) {
            throw new InvalidProductException("name é obrigatório");
        }
        return new Product(Sku.of(sku), name.trim(), Quantity.of(available), 0L);
    }

    public static Product reconstitute(Sku sku, String name, Quantity available, long version) {
        if (sku == null || available == null) {
            throw new InvalidProductException("product state is incomplete");
        }
        return new Product(sku, name, available, version);
    }

    /**
     * O upsert do catálogo: define o estoque em termos absolutos.
     *
     * Absoluto, e não incremental, porque é uma operação de quem administra o
     * catálogo — "este produto tem 40 unidades" — e não de quem vende. Uma
     * reposição incremental seria outro caso de uso, com outra semântica de
     * repetição: repetir este é inofensivo, repetir um incremento não é.
     */
    public Product redefinedAs(String name, Integer available) {
        Product redefined = define(sku.value(), name, available);
        return new Product(sku, redefined.name, redefined.available, version);
    }

    /**
     * Separa unidades para um pedido.
     *
     * Recusar aqui é o ponto inteiro do agregado: é a diferença entre um sistema
     * que vende o que não tem e um que não vende.
     */
    public void reserve(Quantity quantity) {
        if (!quantity.isPositive()) {
            throw new InvalidProductException("uma reserva precisa de quantidade positiva");
        }
        if (!available.coversAtLeast(quantity)) {
            throw new InsufficientStockException(sku, quantity, available);
        }
        available = available.minus(quantity);
    }

    /** Devolve unidades ao estoque — a compensação de {@link #reserve}. */
    public void release(Quantity quantity) {
        available = available.plus(quantity);
    }

    public boolean covers(Quantity quantity) {
        return available.coversAtLeast(quantity);
    }

    public Sku sku() {
        return sku;
    }

    public String name() {
        return name;
    }

    public Quantity available() {
        return available;
    }

    public long version() {
        return version;
    }

    /** Agregados têm identidade: dois Product são o mesmo se têm o mesmo SKU. */
    @Override
    public boolean equals(Object other) {
        return other instanceof Product product && sku.equals(product.sku);
    }

    @Override
    public int hashCode() {
        return sku.hashCode();
    }

    @Override
    public String toString() {
        return "Product[" + sku + " name=" + name + " available=" + available + " v" + version + "]";
    }
}
