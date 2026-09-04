package dev.joaolaureano.trainingkafka.inventory.domain.port;

import dev.joaolaureano.trainingkafka.inventory.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Reservation;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;

import java.util.List;
import java.util.Optional;

/**
 * O estoque, as reservas e os fatos que este contexto ainda deve ao mundo.
 *
 * As três coisas cabem no mesmo Port pelo mesmo motivo que no order-service:
 * {@link #save(Reservation, Product, List)} é a promessa de que descontar o
 * estoque, registrar a reserva e enfileirar o evento acontecem num commit só. Sem
 * essa promessa, um processo morto no meio deixaria estoque descontado que
 * ninguém jamais devolveria — ou, pior, um pedido cobrado sobre uma reserva que
 * não existe.
 */
public interface InventoryRepository {

    Optional<Product> findBySku(Sku sku);

    List<Product> findAll();

    /** A chave da idempotência: um pedido tem no máximo uma reserva. */
    Optional<Reservation> findReservation(String orderId);

    /** Upsert do catálogo — não participa da Saga e não gera evento. */
    void saveProduct(Product product);

    /**
     * Grava a reserva, o novo estado do produto e os eventos, atomicamente.
     *
     * @throws ConcurrentStockChangeException se o produto mudou desde a leitura
     */
    void save(Reservation reservation, Product product, List<DomainEvent> events);

    /**
     * Grava só a reserva e os eventos — usado na rejeição por produto inexistente,
     * em que não há estoque a alterar.
     */
    void save(Reservation reservation, List<DomainEvent> events);
}
