package com.jeel.logging.processor.kafka;

import com.jeel.logging.processor.alert.AlertEvaluateEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AlertEvaluationPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AlertEvaluationPublisher(
            KafkaTemplate<String, Object> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String tenantId, String groupId, String serviceName) {

        AlertEvaluateEvent event =
                new AlertEvaluateEvent(tenantId, groupId);

        event.setServiceName(serviceName);

        kafkaTemplate.send(
                "alerts.evaluate.v1",
                tenantId,
                event
        );
    }
}