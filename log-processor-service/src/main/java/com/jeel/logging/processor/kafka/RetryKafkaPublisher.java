package com.jeel.logging.processor.kafka;

import com.jeel.logging.common.events.NormalizedLogEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Component
public class RetryKafkaPublisher {

    private final KafkaTemplate<String, NormalizedLogEvent> kafkaTemplate;

    public RetryKafkaPublisher(KafkaTemplate<String, NormalizedLogEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishToRetry(NormalizedLogEvent event, int retryCount) {

        String topic;
        long delayMillis;

        if (retryCount == 1) {
            topic = "logs.retry.1m.v1";
            delayMillis = 60_000;
        } else if (retryCount == 2) {
            topic = "logs.retry.5m.v1";
            delayMillis = 5 * 60_000;
        } else if (retryCount == 3) {
            topic = "logs.retry.30m.v1";
            delayMillis = 30 * 60_000;
        } else {
            throw new IllegalArgumentException("Invalid retry count");
        }

        long retryAt = Instant.now().toEpochMilli() + delayMillis;

        ProducerRecord<String, NormalizedLogEvent> record =
                new ProducerRecord<>(topic, event.getTenantId(), event);

        record.headers().add(
                new RecordHeader("x-retry-count",
                        String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8)));

        record.headers().add(
                new RecordHeader("x-retry-at",
                        String.valueOf(retryAt).getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record);
    }
    public void publishToNormalized(
            NormalizedLogEvent event,
            Headers originalHeaders
    ) {

        ProducerRecord<String, NormalizedLogEvent> record =
                new ProducerRecord<>(
                        "logs.normalized.v1",
                        event.getTenantId(),
                        event
                );

        // Preserve retry-count header
        Header retryHeader = originalHeaders.lastHeader("x-retry-count");
        if (retryHeader != null) {
            record.headers().add("x-retry-count", retryHeader.value());
        }

        kafkaTemplate.send(record);
    }
}