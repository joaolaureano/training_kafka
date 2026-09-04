package dev.joaolaureano.trainingkafka.inventory.domain.model;

import java.util.Objects;

/**
 * O identificador do produto no catálogo.
 *
 * É o mesmo valor que viaja no campo {@code product} de um pedido — o pedido não
 * conhece um "id de produto" separado. Por isso a normalização (trim) mora aqui:
 * "  TECLADO " e "TECLADO" precisam ser o mesmo produto, senão dois pedidos
 * digitados de formas diferentes consumiriam estoques diferentes.
 */
public record Sku(String value) {

    public Sku {
        if (value == null || value.isBlank()) {
            throw new InvalidProductException("sku é obrigatório");
        }
        value = value.trim();
        if (value.length() > 64) {
            throw new InvalidProductException("sku deve ter no máximo 64 caracteres");
        }
    }

    public static Sku of(String value) {
        return new Sku(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Sku sku && Objects.equals(value, sku.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
