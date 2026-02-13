package com.jeel.logging.processor.kafka;

import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.processor.retry.RetryTopicConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Component
public class RetryKafkaPublisher {

    private static final String RETRY_HEADER = "x-retry-count";
    private static final String TOPIC = "logs.retry.v1";
    private static final Logger log =
            LoggerFactory.getLogger(RetryKafkaPublisher.class);

    private final KafkaTemplate<String, LogIngestedEvent> kafkaTemplate;

    public RetryKafkaPublisher(KafkaTemplate<String, LogIngestedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishWithRetryHeader(LogIngestedEvent event, int retryCount) {

        ProducerRecord<String, LogIngestedEvent> record =
                new ProducerRecord<>(TOPIC, event.getTenantId(), event);

        record.headers().add(
                new RecordHeader(
                        RETRY_HEADER,
                        String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8)
                )
        );

        kafkaTemplate.send(record);
    }

    public static int extractRetryCount(Iterable<Header> headers) {

        Optional<Header> header = Optional.empty();

        for (Header h : headers) {
            if (RETRY_HEADER.equals(h.key())) {
                header = Optional.of(h);
                break;
            }
        }

        if (header.isPresent()) {
            String value = new String(header.get().value(), StandardCharsets.UTF_8);
            return Integer.parseInt(value);
        }

        return 0;
    }

    public void publishBackToMain(LogIngestedEvent event) {

        kafkaTemplate.send(
                "logs.ingested.v1",
                event.getTenantId(),
                event
        );

        log.info("✅ Sent event back to main topic for retry processing");
    }

}
