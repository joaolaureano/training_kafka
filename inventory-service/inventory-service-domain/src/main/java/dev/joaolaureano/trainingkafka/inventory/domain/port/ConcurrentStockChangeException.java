package dev.joaolaureano.trainingkafka.inventory.domain.port;

/**
 * Alguém alterou o produto entre a leitura e a gravação.
 *
 * É o bloqueio otimista falando: a linha não estava mais na versão em que este
 * agregado foi lido, então a decisão tomada em cima dela pode não valer mais.
 * Não é erro — é o resultado esperado de dois pedidos disputando a mesma última
 * unidade. Quem trata relê e decide de novo.
 */
public class ConcurrentStockChangeException extends RuntimeException {

    public ConcurrentStockChangeException(String message) {
        super(message);
    }
}
