package dev.joaolaureano.trainingkafka.orders.adapters.web;

import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    private final FindOrderPort findOrder;

    public OrderController(PlaceOrderPort placeOrder, FindOrderPort findOrder) {
        this.placeOrder = placeOrder;
        this.findOrder = findOrder;
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

    /**
     * O desfecho da Saga, que o 202 não podia prometer.
     *
     * PENDING_PAYMENT enquanto o pagamento não respondeu; PAID ou CANCELLED
     * depois. É a única forma de observar de fora que a compensação aconteceu.
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> byId(@PathVariable String orderId) {
        return findOrder.byId(orderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
