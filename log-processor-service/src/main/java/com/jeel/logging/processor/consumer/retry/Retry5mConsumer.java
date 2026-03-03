package com.jeel.logging.processor.consumer.retry;

import com.jeel.logging.common.events.NormalizedLogEvent;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;
import com.jeel.logging.processor.kafka.RetryKafkaPublisher;
import com.jeel.logging.processor.retry.RetryDecider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class Retry5mConsumer extends AbstractRetryConsumer {

    public Retry5mConsumer(
            RetryKafkaPublisher retryKafkaPublisher,
            DlqKafkaPublisher dlqKafkaPublisher,
            RetryDecider retryDecider
    ) {
        super(retryKafkaPublisher, dlqKafkaPublisher, retryDecider);
    }

    @KafkaListener(
            topics = "logs.retry.5m.v1",
            containerFactory = "normalizedKafkaListenerContainerFactory",
            groupId = "log-processor-retry-5m"
    )
    public void consume(
            ConsumerRecord<String, NormalizedLogEvent> record,
            Acknowledgment ack
    ) {

        if (!isReadyToProcess(record)) {
            return;
        }

        try {
            // Forward back to normalized topic
            retryKafkaPublisher.publishToNormalized(record.value(), record.headers());
        } catch (Exception ex) {
            handleFailure(record.value(), record, ex);
        }

        ack.acknowledge();
    }
}