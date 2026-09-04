package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

/**
 * O que o listener precisa que alguém faça por ele.
 *
 * Declarado aqui, do lado de quem consome: assim o adapter compila sem conhecer o
 * módulo -application, e quem atende é decidido no bootstrap.
 */
public interface OrderPlacedPort {

    void handle(OrderPlacedMessage message);
}
