package dev.joaolaureano.trainingkafka.orders.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Ponto de entrada do processo.
 *
 * O scan cobre o contexto inteiro ({@code ...orders}) porque as classes de borda
 * — controllers, listeners, handlers de exceção — moram no módulo -adapters, num
 * pacote irmão deste. Elas se registram sozinhas; o que NÃO se registra sozinho
 * é a decisão de quem implementa cada Port, e essa mora em
 * {@link dev.joaolaureano.trainingkafka.orders.bootstrap.config.OrderServiceWiring}.
 */
@SpringBootApplication(scanBasePackages = "dev.joaolaureano.trainingkafka.orders")
public class OrderServiceBootstrap {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceBootstrap.class, args);
    }
}
