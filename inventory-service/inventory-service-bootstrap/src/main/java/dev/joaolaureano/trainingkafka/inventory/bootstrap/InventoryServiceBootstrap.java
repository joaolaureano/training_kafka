package dev.joaolaureano.trainingkafka.inventory.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "dev.joaolaureano.trainingkafka.inventory")
@EnableScheduling // o relay do outbox roda em intervalo fixo
public class InventoryServiceBootstrap {
    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceBootstrap.class, args);
    }
}
