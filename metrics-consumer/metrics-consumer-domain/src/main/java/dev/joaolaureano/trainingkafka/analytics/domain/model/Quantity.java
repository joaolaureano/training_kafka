package dev.joaolaureano.trainingkafka.analytics.domain.model;

/**
 * Quantidade de uma linha de pedido — sempre positiva.
 *
 * Deliberadamente distinta de {@link Units}: "quantas unidades este pedido pediu"
 * e "quantas unidades já foram vendidas no total" são conceitos diferentes, com
 * invariantes diferentes. Um pedido de zero unidades não existe; um produto com
 * zero unidades vendidas existe o tempo todo.
 */
public record Quantity(int value) {

    public Quantity {
        if (value <= 0) {
            throw new InvalidValueException("quantity deve ser maior que zero, recebido: " + value);
        }
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }
}
