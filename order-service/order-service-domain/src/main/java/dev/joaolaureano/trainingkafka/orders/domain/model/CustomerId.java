package dev.joaolaureano.trainingkafka.orders.domain.model;

/** Identifica o cliente que faz o pedido. Também é a chave de particionamento no Kafka. */
public record CustomerId(String value) {

    public CustomerId {
        if (value == null || value.isBlank()) {
            throw new InvalidOrderException("customerId é obrigatório");
        }
        value = value.trim();
    }

    @Override
    public String toString() {
        return value;
    }
}
