package dev.joaolaureano.trainingkafka.inventory.adapters.web;

/**
 * Payload HTTP de entrada — puro transporte, sem uma única regra.
 *
 * Sem Bean Validation de propósito: anotar {@code @PositiveOrZero} aqui criaria
 * uma segunda cópia de uma regra que já vive em {@code Product.define} e em
 * {@code Quantity}, livre para divergir dela com o tempo.
 *
 * O SKU não está no corpo: ele é o identificador do recurso e vem na URL, que é o
 * que torna o PUT idempotente.
 */
public record UpsertProductRequest(String name, Integer available) {
}
