package com.jeel.logging.processor.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.jeel.logging.processor.idempotency.ProcessedEventStore;
import com.jeel.logging.processor.validation.LogIngestedEventValidator;
import com.jeel.logging.processor.kafka.RetryKafkaPublisher;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.common.events.LogEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;

@Component
public class LogIngestedEventConsumer {

    private final ProcessedEventStore processedEventStore;
    private final LogIngestedEventValidator validator;
    private final RetryKafkaPublisher retryKafkaPublisher;
    private final DlqKafkaPublisher dlqKafkaPublisher;


    public LogIngestedEventConsumer(
            ProcessedEventStore processedEventStore,
            LogIngestedEventValidator validator,
            RetryKafkaPublisher retryKafkaPublisher,
            DlqKafkaPublisher dlqKafkaPublisher
    ) {
        this.processedEventStore = processedEventStore;
        this.validator = validator;
        this.retryKafkaPublisher = retryKafkaPublisher;
        this.dlqKafkaPublisher = dlqKafkaPublisher;
    }



    private static final Logger log =
            LoggerFactory.getLogger(LogIngestedEventConsumer.class);

    @KafkaListener(
            topics = "logs.ingested.v1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            LogIngestedEvent event,
            Acknowledgment ack,
            ConsumerRecord<String, LogIngestedEvent> record
    ) {

        int retryCount = RetryKafkaPublisher.extractRetryCount(record.headers());

        String safeTenantId = event.getTenantId() != null
                ? event.getTenantId()
                : "UNKNOWN";

        String eventId = safeTenantId + ":" + event.getRequestId();

        try {

            validator.validate(event);

            if (processedEventStore.isProcessed(eventId)) {
                log.warn("⚠️ Duplicate event ignored | eventId={}", eventId);
                ack.acknowledge();
                return;
            }

            log.info("Processing event | tenant={} | service={} | logs={}",
                    event.getTenantId(),
                    event.getServiceName(),
                    event.getLogs().size());

            for (LogEvent logEvent : event.getLogs()) {
                processSingleLog(event, logEvent);
            }

            processedEventStore.markProcessed(eventId);
            ack.acknowledge();

        } catch (Exception ex) {

            log.error("Processing failed | eventId={} | retryCount={}",
                    event.getTenantId() + ":" + event.getRequestId(),
                    retryCount,
                    ex);

            int nextRetryCount = retryCount + 1;

            if (nextRetryCount <= 3) {

                log.warn("Retrying event | eventId={} | attempt={}",
                        event.getTenantId() + ":" + event.getRequestId(),
                        nextRetryCount);

                retryKafkaPublisher.publishWithRetryHeader(event, nextRetryCount);
                ack.acknowledge();

            } else {

            log.error("Max retries exceeded | routing to DLQ | eventId={}",
                    eventId);

            dlqKafkaPublisher.publishToDlq(
                    event,
                    ex.getClass().getSimpleName() + ": " + ex.getMessage(),
                    retryCount
            );

            ack.acknowledge();
        }

    }
    }


    private void processSingleLog(LogIngestedEvent batch, LogEvent logEvent) {

        if ("ERROR".equalsIgnoreCase(logEvent.getLevel())) {

            String fingerprint = generateFingerprint(batch, logEvent);

            log.warn(
                    "ERROR detected | service={} | fingerprint={}",
                    batch.getServiceName(),
                    fingerprint
            );
        }
    }

    private String generateFingerprint(LogIngestedEvent batch, LogEvent logEvent) {
        return Integer.toHexString(
                (batch.getServiceName()
                        + batch.getEnvironment()
                        + logEvent.getException()).hashCode()
        );
    }
}
