package com.jeel.logging.processor.alert;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AlertNotificationProducer {

    private static final String TOPIC = "alerts.notifications.v1";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AlertNotificationProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Object event, String tenantId) {

        kafkaTemplate.send(TOPIC, tenantId, event);

    }
}