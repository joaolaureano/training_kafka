package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import java.math.BigDecimal;

/**
 * O JSON como ele chega do tópico "orders".
 *
 * Esta é a classe do App B, não do App A — os dois contextos têm cópias próprias
 * do contrato, de propósito. Compartilhar uma classe entre eles os colaria pelo
 * classpath: uma refatoração interna no App A quebraria a compilação do App B, e
 * o modelo de análise ficaria refém do modelo de vendas.
 *
 * O preço são seis nomes de campo repetidos; o retorno é que os dois contextos
 * evoluem sozinhos. Há um teste de contrato ({@code OrderPlacedMessageTest}) que
 * falha se o formato do fio mudar sem que alguém perceba.
 */
public record OrderPlacedMessage(
        String orderId,
        String customerId,
        String product,
        Integer quantity,
        BigDecimal amount,
        String occurredAt
) {
}
