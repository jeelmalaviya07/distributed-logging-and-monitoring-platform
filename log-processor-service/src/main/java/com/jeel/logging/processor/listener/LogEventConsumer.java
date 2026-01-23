package com.jeel.logging.processor.listener;

import com.jeel.logging.common.events.LogIngestedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class LogEventConsumer {

    @KafkaListener(
            topics = "logs.ingested.v1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void process(LogIngestedEvent event, Acknowledgment ack) {

        System.out.println("🔥 RECEIVED EVENT FROM KAFKA");
        System.out.println("Tenant = " + event.getTenantId());
        System.out.println("Service = " + event.getServiceName());
        System.out.println("Environment = " + event.getEnvironment());
        System.out.println("Logs count = " + event.getLogs().size());

        // 🔥 For now just print each log
        event.getLogs().forEach(log -> {
            System.out.println("➡️ [" + log.getLevel() + "] " + log.getMessage());
        });

        // VERY IMPORTANT — commit offset after successful processing
        ack.acknowledge();

        System.out.println("✅ EVENT PROCESSED AND OFFSET COMMITTED\n");
    }
}
