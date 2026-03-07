package com.jeel.logging.processor.consumer.retry;

import com.jeel.logging.common.events.NormalizedLogEvent;
import com.jeel.logging.processor.kafka.RetryKafkaPublisher;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;
import com.jeel.logging.processor.retry.RetryDecider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalTime;

@Component
public class Retry1mConsumer extends AbstractRetryConsumer {

    public Retry1mConsumer(
            RetryKafkaPublisher retryKafkaPublisher,
            DlqKafkaPublisher dlqKafkaPublisher,
            RetryDecider retryDecider
    ) {
        super(retryKafkaPublisher, dlqKafkaPublisher, retryDecider);
    }

    @KafkaListener(
            topics = "logs.retry.1m.v1",
            containerFactory = "normalizedKafkaListenerContainerFactory",
            groupId = "log-processor-retry-1m"
    )
    public void consume(
            ConsumerRecord<String, NormalizedLogEvent> record,
            Acknowledgment ack
    ) {
        Logger log =
                LoggerFactory.getLogger(Retry1mConsumer.class);

        if (!isReadyToProcess(record)) {
            log.info("Now={}, retryAt={}", System.currentTimeMillis(), LocalTime.now());
            // Not ready yet → do not ack
            return;
        }

        try {
            // Send BACK to normalized topic for actual retry attempt
            retryKafkaPublisher.publishToNormalized(record.value(), record.headers());
        } catch (Exception ex) {
            handleFailure(record.value(), record, ex);
        }

        ack.acknowledge();
    }
}