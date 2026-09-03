package dev.joaolaureano.trainingkafka.payment.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "dev.joaolaureano.trainingkafka.payment")
@EnableScheduling // o relay do outbox roda em intervalo fixo
public class PaymentServiceBootstrap {
    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceBootstrap.class, args);
    }
}
