package com.jeel.logging.ingestion.kafka.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.ingestion.port.LogEventPublisher;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaLogEventPublisher implements LogEventPublisher {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaLogEventPublisher.class);

    private static final String TOPIC = "logs.ingested.v1";

    private final KafkaTemplate<String, LogIngestedEvent> kafkaTemplate;

    public KafkaLogEventPublisher(KafkaTemplate<String, LogIngestedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(LogIngestedEvent event) {
        try {
            System.out.println("🔥 PUBLISHING TO KAFKA | tenant=" + event.getTenantId() +
                    " | logs=" + event.getLogs().size());

            CompletableFuture<SendResult<String, LogIngestedEvent>> future =
                    kafkaTemplate.send(TOPIC, event.getTenantId(), event);

            System.out.println("✅ SEND CALLED SUCCESSFULLY");

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error(
                            "Kafka publish failed | tenantId={} | requestId={}",
                            event.getTenantId(),
                            event.getRequestId(),
                            ex
                    );
                }
                // success path intentionally empty (metrics later)
            });

        } catch (Exception e) {
            // immediate failure (serialization, buffer full, config)
            throw new RuntimeException("Kafka producer rejected event", e);
        }
    }
}
