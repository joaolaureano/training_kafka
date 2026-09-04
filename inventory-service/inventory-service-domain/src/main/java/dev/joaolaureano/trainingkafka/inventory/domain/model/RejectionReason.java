package dev.joaolaureano.trainingkafka.inventory.domain.model;

/**
 * Por que uma reserva não pôde ser feita.
 *
 * São dois casos genuinamente diferentes, e achatá-los num "não deu" faria o
 * pedido cancelado dizer a mesma coisa para "esse produto não existe" e para
 * "acabou o estoque" — que são problemas de quem opera o catálogo e de quem
 * repõe, respectivamente.
 */
public enum RejectionReason {

    /** O SKU não está no catálogo. */
    UNKNOWN_PRODUCT,

    /** O produto existe, mas não há unidades suficientes. */
    OUT_OF_STOCK
}
