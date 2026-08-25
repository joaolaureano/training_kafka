package dev.joaolaureano.trainingkafka.orders.adapters.web;

import java.time.Instant;
import java.util.List;

public record ApiError(String error, List<String> details, Instant timestamp) {

    public static ApiError of(String error, List<String> details) {
        return new ApiError(error, details, Instant.now());
    }
}
