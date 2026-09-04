package dev.joaolaureano.trainingkafka.inventory.domain.model;

/**
 * Não há unidades suficientes para atender a reserva.
 *
 * É exceção, e não um retorno booleano, porque quem chama {@code reserve} não tem
 * escolha legítima de ignorar o resultado: reservar sem estoque é a única coisa
 * que este agregado existe para impedir.
 */
public class InsufficientStockException extends RuntimeException {

    private final Sku sku;
    private final Quantity requested;
    private final Quantity available;

    public InsufficientStockException(Sku sku, Quantity requested, Quantity available) {
        super("estoque insuficiente para " + sku + ": pedidas " + requested
                + ", disponíveis " + available);
        this.sku = sku;
        this.requested = requested;
        this.available = available;
    }

    public Sku sku() {
        return sku;
    }

    public Quantity requested() {
        return requested;
    }

    public Quantity available() {
        return available;
    }
}
