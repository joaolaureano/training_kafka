package dev.joaolaureano.trainingkafka.fraud.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "dev.joaolaureano.trainingkafka.fraud")
public class FraudServiceBootstrap {

    public static void main(String[] args) {
        SpringApplication.run(FraudServiceBootstrap.class, args);
    }
}
