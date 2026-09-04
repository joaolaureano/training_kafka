package dev.joaolaureano.trainingkafka.orders.domain.model;

import dev.joaolaureano.trainingkafka.orders.domain.event.OrderPlaced;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Nenhum @SpringBootTest, nenhum contexto, nenhum mock: milissegundos para rodar.
 * Isso só é possível porque o domínio não depende de nada — é o retorno prático
 * de tê-lo isolado num módulo próprio.
 */
class OrderTest {

    private static final Instant NOW = Instant.parse("2026-08-25T12:00:00Z");

    private static Order validOrder() {
        return Order.place("cust-1", "Teclado", 2, new BigDecimal("199.90"), NOW);
    }

    /**
     * Um pedido no ponto em que a cobrança pode acontecer.
     *
     * Desde que o estoque entrou na Saga, PENDING_PAYMENT não é mais o estado
     * inicial: é o segundo. Os testes de pagamento partem daqui.
     */
    private static Order reservedOrder() {
        Order order = validOrder();
        order.confirmStock();
        return order;
    }

    @Nested
    @DisplayName("ao registrar um pedido válido")
    class WhenValid {

        @Test
        @DisplayName("gera identidade própria e preserva os dados")
        void createsOrder() {
            Order order = validOrder();

            assertThat(order.id()).isNotNull();
            assertThat(order.customerId().value()).isEqualTo("cust-1");
            assertThat(order.quantity().value()).isEqualTo(2);
            assertThat(order.amount().amount()).isEqualByComparingTo("199.90");
            assertThat(order.placedAt()).isEqualTo(NOW);
            assertThat(order.status()).isEqualTo(OrderStatus.PENDING_STOCK);
        }

        @Test
        @DisplayName("um pedido novo espera o estoque, não o pagamento")
        void startsWaitingForStock() {
            assertThat(validOrder().status()).isEqualTo(OrderStatus.PENDING_STOCK);
        }

        @Test
        @DisplayName("estoque reservado libera a cobrança, e reentrega não muda nada")
        void stockConfirmationIsIdempotent() {
            Order order = validOrder();

            order.confirmStock();
            order.confirmStock();

            assertThat(order.status()).isEqualTo(OrderStatus.PENDING_PAYMENT);
        }

        @Test
        @DisplayName("sem estoque, o pedido morre antes de qualquer cobrança")
        void outOfStockCancelsBeforePayment() {
            Order order = validOrder();

            order.cancelForOutOfStock();
            order.cancelForOutOfStock();

            assertThat(order.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("resultado de pagamento em PENDING_STOCK é atraso de tópico, não contradição")
        void paymentResultIsAcceptedWhileStillPendingStock() {
            /*
             * Os dois fatos vêm de tópicos diferentes e o Kafka não os ordena entre si.
             * Se existe um resultado de pagamento, o estoque FOI reservado — o
             * payment-service só é disparado por StockReserved. Recusar aqui mandava um
             * resultado legítimo para a DLQ e deixava o pedido preso para sempre.
             */
            Order approved = validOrder();
            approved.approvePayment();
            assertThat(approved.status()).isEqualTo(OrderStatus.PAID);

            Order failed = validOrder();
            failed.cancelForPaymentFailure();
            assertThat(failed.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("StockReserved que chega depois do pagamento é a mesma verdade atrasada")
        void lateStockConfirmationIsANoop() {
            Order order = validOrder();
            order.approvePayment();

            order.confirmStock();

            assertThat(order.status())
                    .as("não pode desfazer o pagamento já aplicado")
                    .isEqualTo(OrderStatus.PAID);
        }

        @Test
        @DisplayName("StockReserved nunca ressuscita um pedido cancelado")
        void stockCannotResurrectACancelledOrder() {
            Order cancelled = validOrder();
            cancelled.cancelForOutOfStock();

            cancelled.confirmStock();

            assertThat(cancelled.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("falta de estoque depois de cobrado continua sendo contradição")
        void stockRejectionAfterPaymentIsStillRejected() {
            /*
             * Aqui a guarda estrita é legítima: uma reserva é decidida uma vez só, então
             * StockRejected e StockReserved são mutuamente exclusivos para o mesmo
             * pedido. Isto não é atraso de tópico — é impossível.
             */
            Order paid = reservedOrder();
            paid.approvePayment();

            assertThatThrownBy(paid::cancelForOutOfStock)
                    .isInstanceOf(InvalidOrderTransitionException.class);
            assertThat(paid.status()).isEqualTo(OrderStatus.PAID);
        }

        @Test
        @DisplayName("reaplicar o mesmo resultado de pagamento não muda nada")
        void paymentResultIsIdempotent() {
            Order approved = reservedOrder();
            approved.approvePayment();
            approved.approvePayment();
            assertThat(approved.status()).isEqualTo(OrderStatus.PAID);

            Order cancelled = reservedOrder();
            cancelled.cancelForPaymentFailure();
            cancelled.cancelForPaymentFailure();
            assertThat(cancelled.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("resultados contraditórios são recusados, não engolidos")
        void contradictoryResultsAreRejected() {
            Order paid = reservedOrder();
            paid.approvePayment();
            assertThatThrownBy(paid::cancelForPaymentFailure)
                    .isInstanceOf(InvalidOrderTransitionException.class);
            assertThat(paid.status()).isEqualTo(OrderStatus.PAID);

            Order cancelled = reservedOrder();
            cancelled.cancelForPaymentFailure();
            assertThatThrownBy(cancelled::approvePayment)
                    .isInstanceOf(InvalidOrderTransitionException.class);
            assertThat(cancelled.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("fraude cancela até um pedido já pago — a única saída de PAID")
        void fraudCancelsEvenAPaidOrder() {
            Order order = reservedOrder();
            order.approvePayment();

            order.cancelForFraud();

            assertThat(order.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("fraude reentregue não muda nada")
        void fraudCancellationIsIdempotent() {
            Order order = reservedOrder();
            order.approvePayment();

            order.cancelForFraud();
            order.cancelForFraud();

            assertThat(order.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("cancelar por fraude é permitido onde cancelar por falha seria contradição")
        void fraudAndPaymentFailureAreDifferentTransitions() {
            Order paid = reservedOrder();
            paid.approvePayment();

            assertThatThrownBy(paid::cancelForPaymentFailure)
                    .isInstanceOf(InvalidOrderTransitionException.class);
            paid.cancelForFraud();

            assertThat(paid.status()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("reconstitui do banco preservando o estado")
        void reconstitutesFromStorage() {
            Order order = reservedOrder();
            order.approvePayment();

            Order loaded = Order.reconstitute(order.id(), order.customerId(), order.productId(),
                    order.quantity(), order.amount(), order.placedAt(), order.status());

            assertThat(loaded.status()).isEqualTo(OrderStatus.PAID);
            assertThat(loaded).isEqualTo(order);
            assertThat(loaded.pullDomainEvents()).isEmpty();
        }

        @Test
        @DisplayName("registra o fato OrderPlaced")
        void recordsDomainEvent() {
            Order order = validOrder();

            assertThat(order.pendingEvents())
                    .singleElement()
                    .isInstanceOfSatisfying(OrderPlaced.class, event -> {
                        assertThat(event.orderId()).isEqualTo(order.id());
                        assertThat(event.customerId()).isEqualTo(order.customerId());
                        assertThat(event.occurredAt()).isEqualTo(NOW);
                    });
        }

        @Test
        @DisplayName("pullDomainEvents drena a lista, para o fato não ser publicado duas vezes")
        void drainsEventsOnce() {
            Order order = validOrder();

            assertThat(order.pullDomainEvents()).hasSize(1);
            assertThat(order.pullDomainEvents()).isEmpty();
        }

        @Test
        @DisplayName("dois pedidos distintos nunca colidem em identidade")
        void identityIsUnique() {
            assertThat(validOrder()).isNotEqualTo(validOrder());
        }
    }

    @Nested
    @DisplayName("ao violar uma invariante")
    class WhenInvalid {

        @Test
        @DisplayName("recusa valor zero — Money aceita, mas um pedido não")
        void rejectsZeroAmount() {
            assertThatThrownBy(() -> Order.place("cust-1", "Teclado", 1, BigDecimal.ZERO, NOW))
                    .isInstanceOf(InvalidOrderException.class)
                    .hasMessageContaining("maior que zero");
        }

        @Test
        @DisplayName("acumula TODAS as violações, em vez de parar na primeira")
        void accumulatesEveryViolation() {
            assertThatThrownBy(() -> Order.place("  ", null, -1, new BigDecimal("-5"), NOW))
                    .isInstanceOfSatisfying(InvalidOrderException.class, rejection ->
                            assertThat(rejection.violations())
                                    .extracting(Violation::field)
                                    .containsExactly("customerId", "product", "quantity", "amount"));
        }

        @Test
        @DisplayName("distingue campo ausente de campo inválido")
        void distinguishesMissingFromInvalid() {
            assertThatThrownBy(() -> Order.place("cust-1", "Teclado", null, new BigDecimal("10"), NOW))
                    .isInstanceOfSatisfying(InvalidOrderException.class, rejection ->
                            assertThat(rejection.violations())
                                    .singleElement()
                                    .satisfies(v -> {
                                        assertThat(v.field()).isEqualTo("quantity");
                                        assertThat(v.message()).contains("obrigatório");
                                    }));
        }

        @Test
        @DisplayName("recusa quantidade não positiva")
        void rejectsNonPositiveQuantity() {
            assertThatThrownBy(() -> new Quantity(0))
                    .isInstanceOf(InvalidOrderException.class);
            assertThatThrownBy(() -> new Quantity(-3))
                    .isInstanceOf(InvalidOrderException.class);
        }

        @Test
        @DisplayName("recusa valor negativo")
        void rejectsNegativeAmount() {
            assertThatThrownBy(() -> Money.of("-0.01"))
                    .isInstanceOf(InvalidOrderException.class);
        }

        @Test
        @DisplayName("recusa identificadores vazios")
        void rejectsBlankIdentifiers() {
            assertThatThrownBy(() -> new CustomerId("   "))
                    .isInstanceOf(InvalidOrderException.class);
            assertThatThrownBy(() -> new ProductId(null))
                    .isInstanceOf(InvalidOrderException.class);
        }
    }

    @Nested
    @DisplayName("Money")
    class MoneyRules {

        @Test
        @DisplayName("normaliza para duas casas decimais")
        void normalizesScale() {
            assertThat(Money.of("10").toString()).isEqualTo("10.00");
            assertThat(Money.of("10.005").toString()).isEqualTo("10.01");
        }

        @Test
        @DisplayName("soma e multiplica preservando a escala")
        void arithmetic() {
            assertThat(Money.of("10.50").plus(Money.of("4.50")).toString()).isEqualTo("15.00");
            assertThat(Money.of("10.50").times(new Quantity(3)).toString()).isEqualTo("31.50");
        }

        @Test
        @DisplayName("igualdade é por valor, não por identidade")
        void equalityIsByValue() {
            assertThat(Money.of("10.00")).isEqualTo(Money.of("10"));
        }
    }
}
