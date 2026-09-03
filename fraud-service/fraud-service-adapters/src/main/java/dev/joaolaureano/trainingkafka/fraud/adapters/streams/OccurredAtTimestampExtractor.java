package dev.joaolaureano.trainingkafka.fraud.adapters.streams;

import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.OrderPlacedMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class OccurredAtTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (!(record.value() instanceof OrderPlacedMessage message)) {
            return partitionTime >= 0 ? partitionTime : record.timestamp();
        }
        return Instant.parse(message.occurredAt()).toEpochMilli();
    }
}
