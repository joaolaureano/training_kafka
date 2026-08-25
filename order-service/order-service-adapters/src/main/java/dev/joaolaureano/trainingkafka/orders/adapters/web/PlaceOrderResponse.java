package dev.joaolaureano.trainingkafka.orders.adapters.web;

public record PlaceOrderResponse(String orderId, String status) {

    public static PlaceOrderResponse accepted(String orderId) {
        return new PlaceOrderResponse(orderId, "ACCEPTED");
    }
}
