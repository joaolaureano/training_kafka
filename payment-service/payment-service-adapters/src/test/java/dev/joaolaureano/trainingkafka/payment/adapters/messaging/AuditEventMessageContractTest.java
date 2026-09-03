package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O contrato de `audit-events` como o App C o lê.
 *
 * Não há classe compartilhada entre os dois serviços — de propósito — então nada
 * no compilador impede alguém de renomear um campo aqui. O que sobra é este
 * teste: os nomes abaixo são exatamente os do record que o audit-service
 * desserializa, e mudar qualquer um deles manda todo log deste serviço para a
 * DLQ, em silêncio, em produção.
 */
class AuditEventMessageContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("o JSON tem exatamente os campos que o audit-service espera")
    void serializesToTheAgreedShape() throws Exception {
        Map<String, String> context = new LinkedHashMap<>();
        context.put("orderId", "order-1");
        context.put("amount", "42.00");

        AuditEventMessage message = new AuditEventMessage("WARN", "2026-09-03T12:00:00Z",
                "payment-service", "payment.refunded", context);

        var json = objectMapper.readTree(objectMapper.writeValueAsString(message));

        assertThat(json.fieldNames()).toIterable()
                .containsExactlyInAnyOrder("level", "timestamp", "app", "action", "context");
        assertThat(json.get("level").asText()).isEqualTo("WARN");
        // ISO-8601: o tradutor do App C usa Instant.parse e recusa qualquer outra coisa.
        assertThat(json.get("timestamp").asText()).isEqualTo("2026-09-03T12:00:00Z");
        assertThat(json.get("app").asText()).isEqualTo("payment-service");
        assertThat(json.get("action").asText()).isEqualTo("payment.refunded");
        assertThat(json.get("context").get("orderId").asText()).isEqualTo("order-1");
    }
}
