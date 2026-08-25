package dev.joaolaureano.trainingkafka.logs.adapters.web;

import dev.joaolaureano.trainingkafka.logs.domain.model.InvalidLogException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

@RestControllerAdvice
public class LogExceptionHandler {

    @ExceptionHandler({InvalidLogException.class, DateTimeParseException.class})
    public ResponseEntity<Map<String, Object>> onInvalidQuery(RuntimeException exception) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "parâmetro de consulta inválido",
                "detail", String.valueOf(exception.getMessage()),
                "timestamp", Instant.now().toString()));
    }
}
