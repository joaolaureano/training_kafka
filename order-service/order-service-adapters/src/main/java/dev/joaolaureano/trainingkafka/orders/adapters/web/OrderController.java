package dev.joaolaureano.trainingkafka.orders.adapters.web;

import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Adapter de entrada HTTP.
 *
 * Recebe a requisição, delega pelo Port que ele mesmo declara
 * ({@link PlaceOrderPort}) e traduz o resultado em resposta. Não decide nada —
 * se aparecer regra de negócio aqui, ela vazou de dentro.
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final PlaceOrderPort placeOrder;

    public OrderController(PlaceOrderPort placeOrder) {
        this.placeOrder = placeOrder;
    }

    /**
     * Responde 202 Accepted, e não 201 Created: o pedido foi aceito para
     * processamento e publicado no tópico, mas as métricas e a análise do App B
     * acontecem de forma assíncrona. Prometer "Created" seria mentir sobre o
     * que já terminou.
     */
    @PostMapping
    public ResponseEntity<PlaceOrderResponse> place(@RequestBody PlaceOrderRequest request) {
        OrderId orderId = placeOrder.place(request);

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(PlaceOrderResponse.accepted(orderId.toString()));
    }
}
