package dev.joaolaureano.trainingkafka.inventory.adapters.web;

import dev.joaolaureano.trainingkafka.inventory.domain.model.InvalidProductException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

/**
 * Traduz exceções em respostas HTTP.
 *
 * A mensagem de recusa vem pronta do domínio — este adapter apenas escolhe o
 * status e formata.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(InvalidProductException.class)
    public ResponseEntity<ApiError> onDomainRejection(InvalidProductException exception) {
        return ResponseEntity.badRequest()
                .body(ApiError.of("produto rejeitado", List.of(exception.getMessage())));
    }

    /** JSON malformado ou tipo incompatível: nem chega a virar um comando. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> onMalformedBody(HttpMessageNotReadableException exception) {
        return ResponseEntity.badRequest()
                .body(ApiError.of("corpo da requisição ilegível",
                        List.of("JSON malformado ou tipo inválido")));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> onUnexpected(Exception exception) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiError.of("erro interno", List.of(exception.getClass().getSimpleName())));
    }
}
