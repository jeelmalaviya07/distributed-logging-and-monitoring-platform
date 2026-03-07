package com.jeel.logging.processor.kafka;

import com.jeel.logging.common.events.NormalizedLogEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class DlqKafkaPublisher {

    private static final String DLQ_TOPIC = "logs.dlq.v1";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public DlqKafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishToDlq(
            NormalizedLogEvent event,
            String failureReason,
            int finalRetryCount
    ) {

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(DLQ_TOPIC, event.getTenantId(), event);

        record.headers().add(
                new RecordHeader(
                        "x-failure-reason",
                        failureReason.getBytes(StandardCharsets.UTF_8)
                )
        );

        record.headers().add(
                new RecordHeader(
                        "x-final-retry-count",
                        String.valueOf(finalRetryCount).getBytes(StandardCharsets.UTF_8)
                )
        );

        kafkaTemplate.send(record);
    }
}