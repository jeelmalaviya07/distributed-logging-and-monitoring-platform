package com.jeel.logging.processor.kafka;

import com.jeel.logging.processor.alert.AlertNotificationEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class AlertNotificationPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AlertNotificationPublisher(
            KafkaTemplate<String, Object> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(AlertNotificationEvent event) {

        kafkaTemplate.send(
                "alerts.notifications.v1",
                event.getTenantId(),
                event
        );
    }
}