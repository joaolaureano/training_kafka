package dev.joaolaureano.trainingkafka.audit.adapters.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.listener.CommonErrorHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A DLQ é uma decisão de configuração, não de código. Este teste prende esse
 * contrato: o que o application.yml diz é o que o contexto monta.
 */
class KafkaErrorHandlingConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
            .withUserConfiguration(KafkaErrorHandlingConfig.class)
            // Sem broker no teste: só queremos ver quais beans o contexto monta.
            .withPropertyValues("spring.kafka.admin.auto-create=false");

    @Test
    void publicaNaDeadLetterPorPadrao() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(CommonErrorHandler.class);
            assertThat(context).hasBean("deadLetterKafkaTemplate");
            assertThat(context.getBean("applicationLogsDeadLetterTopic", NewTopic.class).name())
                    .isEqualTo("audit-events-dlt");
        });
    }

    @Test
    void desligadaVoltaAoDescarteRegistrado() {
        contextRunner.withPropertyValues("kafka.dlq.enabled=false").run(context -> {
            assertThat(context).hasBean("logAndSkipErrorHandler");
            assertThat(context).doesNotHaveBean("deadLetterKafkaTemplate");
        });
    }

    @Test
    void sufixoEParticoesSaoConfiguraveis() {
        contextRunner
                .withPropertyValues("kafka.dlq.topic-suffix=.dead", "kafka.dlq.partitions=1")
                .run(context -> {
                    NewTopic topic = context.getBean("applicationLogsDeadLetterTopic", NewTopic.class);
                    assertThat(topic.name()).isEqualTo("audit-events.dead");
                    assertThat(topic.numPartitions()).isEqualTo(1);
                });
    }

    @Test
    void criacaoDeTopicoPodeSerDesligadaQuandoOBrokerJaOsTem() {
        contextRunner.withPropertyValues("kafka.dlq.create-topics=false")
                .run(context -> assertThat(context).doesNotHaveBean(NewTopic.class));
    }
}
