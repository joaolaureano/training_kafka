package dev.joaolaureano.trainingkafka.logs.adapters;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LogAggregatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogAggregatorApplication.class, args);
    }
}
