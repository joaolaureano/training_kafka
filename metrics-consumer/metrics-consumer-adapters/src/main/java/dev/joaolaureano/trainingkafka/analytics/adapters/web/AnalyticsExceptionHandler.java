package dev.joaolaureano.trainingkafka.analytics.adapters.web;

import dev.joaolaureano.trainingkafka.analytics.domain.model.InvalidValueException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class AnalyticsExceptionHandler {

    @ExceptionHandler(InvalidValueException.class)
    public ResponseEntity<Map<String, Object>> onInvalidValue(InvalidValueException exception) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "parâmetro inválido",
                "detail", exception.getMessage(),
                "timestamp", Instant.now().toString()));
    }
}
