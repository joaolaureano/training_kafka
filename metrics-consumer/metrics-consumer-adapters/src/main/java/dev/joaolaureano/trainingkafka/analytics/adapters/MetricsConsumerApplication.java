package dev.joaolaureano.trainingkafka.analytics.adapters;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MetricsConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricsConsumerApplication.class, args);
    }
}
