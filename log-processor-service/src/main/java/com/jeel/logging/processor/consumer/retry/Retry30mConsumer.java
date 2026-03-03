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
public class Retry30mConsumer extends AbstractRetryConsumer {

    public Retry30mConsumer(
            RetryKafkaPublisher retryKafkaPublisher,
            DlqKafkaPublisher dlqKafkaPublisher,
            RetryDecider retryDecider
    ) {
        super(retryKafkaPublisher, dlqKafkaPublisher, retryDecider);
    }

    @KafkaListener(
            topics = "logs.retry.30m.v1",
            containerFactory = "normalizedKafkaListenerContainerFactory",
            groupId = "log-processor-retry-30m"
    )
    public void consume(
            ConsumerRecord<String, NormalizedLogEvent> record,
            Acknowledgment ack
    ) {

        // 🔹 Check if retry time has arrived
        if (!isReadyToProcess(record)) {
            return;
        }

        NormalizedLogEvent event = record.value();

        try {

            // 🔥 FINAL ATTEMPT
            // We forward back to normalized topic for one last processing attempt
            // (Optional: You can directly process storage logic here instead)

            retryKafkaPublisher.publishToNormalized(record.value(), record.headers());
            // retryCount=4 will cause immediate DLQ in main consumer

        } catch (Exception ex) {

            // 🔥 FINAL FAILURE → DLQ DIRECTLY
            dlqKafkaPublisher.publishToDlq(
                    event,
                    "FINAL_RETRY_FAILED: " + ex.getMessage(),
                    extractRetryCount(record)
            );
        }

        ack.acknowledge();
    }
}