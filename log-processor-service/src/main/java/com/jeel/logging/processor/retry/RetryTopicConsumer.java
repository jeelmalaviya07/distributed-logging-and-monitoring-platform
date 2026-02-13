package com.jeel.logging.processor.retry;

import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.processor.config.RetryConfig;
import com.jeel.logging.processor.kafka.RetryKafkaPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RetryTopicConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(RetryTopicConsumer.class);

    private final RetryConfig retryConfig;
    private final RetryKafkaPublisher retryKafkaPublisher;

    public RetryTopicConsumer(
            RetryConfig retryConfig,
            RetryKafkaPublisher retryKafkaPublisher
    ) {
        this.retryConfig = retryConfig;
        this.retryKafkaPublisher = retryKafkaPublisher;
    }

    @KafkaListener(
            topics = "logs.retry.v1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRetry(
            LogIngestedEvent event,
            Acknowledgment ack,
            ConsumerRecord<String, LogIngestedEvent> record
    ) {

        int retryCount =
                RetryKafkaPublisher.extractRetryCount(record.headers());

        log.warn("⏳ RETRY TOPIC EVENT RECEIVED | eventId={} | retryCount={}",
                event.getTenantId() + ":" + event.getRequestId(),
                retryCount
        );

        try {
            // ✅ Backoff delay
            long delay =
                    retryConfig.getBackoffMs() * retryCount;

            log.info("Sleeping {} ms before retry...", delay);

            Thread.sleep(delay);

            // ✅ Re-send back to main topic
            log.info("Re-publishing event back to main topic...");

            retryKafkaPublisher.publishBackToMain(event);

            ack.acknowledge();

        } catch (Exception ex) {

            log.error("Retry worker failed completely → dropping event", ex);
            ack.acknowledge();
        }
    }
}
