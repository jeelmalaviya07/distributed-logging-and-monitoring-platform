package com.jeel.logging.processor.consumer.retry;

import com.jeel.logging.common.events.NormalizedLogEvent;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;
import com.jeel.logging.processor.kafka.RetryKafkaPublisher;
import com.jeel.logging.processor.retry.RetryDecider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;

public abstract class AbstractRetryConsumer {

    protected final RetryKafkaPublisher retryKafkaPublisher;
    protected final DlqKafkaPublisher dlqKafkaPublisher;
    protected final RetryDecider retryDecider;

    protected AbstractRetryConsumer(
            RetryKafkaPublisher retryKafkaPublisher,
            DlqKafkaPublisher dlqKafkaPublisher,
            RetryDecider retryDecider
    ) {
        this.retryKafkaPublisher = retryKafkaPublisher;
        this.dlqKafkaPublisher = dlqKafkaPublisher;
        this.retryDecider = retryDecider;
    }

    protected boolean isReadyToProcess(ConsumerRecord<String, NormalizedLogEvent> record) {

        Header retryAtHeader = record.headers().lastHeader("x-retry-at");

        if (retryAtHeader == null) {
            return true;
        }

        long retryAt = Long.parseLong(
                new String(retryAtHeader.value(), StandardCharsets.UTF_8));

        return System.currentTimeMillis() >= retryAt;
    }

    protected int extractRetryCount(ConsumerRecord<String, NormalizedLogEvent> record) {

        Header header = record.headers().lastHeader("x-retry-count");

        if (header == null) {
            return 0;
        }

        return Integer.parseInt(
                new String(header.value(), StandardCharsets.UTF_8));
    }

    protected void handleFailure(
            NormalizedLogEvent event,
            ConsumerRecord<String, NormalizedLogEvent> record,
            Exception ex
    ) {

        int retryCount = extractRetryCount(record);
        retryCount++;

        if (!retryDecider.isRetryable(ex) || retryCount > 3) {

            dlqKafkaPublisher.publishToDlq(
                    event,
                    "RETRY_EXHAUSTED: " + ex.getMessage(),
                    retryCount
            );
            return;
        }

        retryKafkaPublisher.publishToRetry(event, retryCount);
    }
}