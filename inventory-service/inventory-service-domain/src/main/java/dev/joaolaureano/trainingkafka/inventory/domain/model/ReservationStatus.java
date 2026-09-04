package dev.joaolaureano.trainingkafka.inventory.domain.model;

public enum ReservationStatus {
    /** Unidades separadas, aguardando o desfecho do pagamento. */
    HELD,
    /** Nunca houve separação: produto inexistente ou sem estoque. */
    REJECTED,
    /** As unidades voltaram ao estoque — compensação da Saga. */
    RELEASED,
    /**
     * O pedido morreu antes de a reserva chegar a existir.
     *
     * Acontece quando a compensação vence a corrida com o próprio pedido: a
     * detecção de fraude cancela um pagamento que ainda não existia, e o
     * PaymentCancelled resultante chega aqui antes do OrderPlaced. Sem registrar
     * nada, este contexto reservaria estoque depois para um pedido já cancelado —
     * e nunca mais o devolveria, porque a mensagem que o devolveria já passou.
     */
    VOIDED
}
