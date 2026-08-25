package dev.joaolaureano.trainingkafka.orders.domain.model;

/**
 * Quantidade de unidades de um pedido. Sempre positiva — não existe pedido de
 * zero ou de menos-três unidades, então esse estado nunca chega a ser representável.
 */
public record Quantity(int value) {

    public Quantity {
        if (value <= 0) {
            throw new InvalidOrderException("quantity deve ser maior que zero, recebido: " + value);
        }
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }
}
