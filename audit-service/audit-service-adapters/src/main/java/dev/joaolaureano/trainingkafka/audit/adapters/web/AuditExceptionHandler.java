package dev.joaolaureano.trainingkafka.audit.adapters.web;

import dev.joaolaureano.trainingkafka.audit.domain.model.InvalidAuditException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

@RestControllerAdvice
public class AuditExceptionHandler {

    @ExceptionHandler({InvalidAuditException.class, DateTimeParseException.class})
    public ResponseEntity<Map<String, Object>> onInvalidQuery(RuntimeException exception) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "parâmetro de consulta inválido",
                "detail", String.valueOf(exception.getMessage()),
                "timestamp", Instant.now().toString()));
    }
}
