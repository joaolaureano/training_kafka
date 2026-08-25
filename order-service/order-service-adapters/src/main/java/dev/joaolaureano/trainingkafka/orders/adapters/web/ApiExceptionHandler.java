package dev.joaolaureano.trainingkafka.orders.adapters.web;

import dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderException;
import dev.joaolaureano.trainingkafka.orders.domain.model.Violation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

/**
 * Traduz exceções em respostas HTTP.
 *
 * Só existe um handler de recusa de pedido, porque só existe um lugar que recusa
 * pedidos. A lista de violações vem pronta do domínio — este adapter apenas
 * escolhe o status 400 e formata.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(InvalidOrderException.class)
    public ResponseEntity<ApiError> onDomainRejection(InvalidOrderException exception) {
        List<String> details = exception.violations().stream()
                .map(Violation::toString)
                .toList();

        return ResponseEntity.badRequest().body(ApiError.of("pedido rejeitado", details));
    }

    /** JSON malformado ou tipo incompatível: nem chega a virar um comando. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> onMalformedBody(HttpMessageNotReadableException exception) {
        return ResponseEntity.badRequest()
                .body(ApiError.of("corpo da requisição ilegível", List.of("JSON malformado ou tipo inválido")));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> onUnexpected(Exception exception) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiError.of("erro interno", List.of(exception.getClass().getSimpleName())));
    }
}
