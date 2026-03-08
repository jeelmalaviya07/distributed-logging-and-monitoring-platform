package com.jeel.notification.kafka;

import com.jeel.notification.model.AlertNotificationEvent;
import com.jeel.notification.service.NotificationDispatcher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    private final NotificationDispatcher dispatcher;

    public NotificationConsumer(NotificationDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @KafkaListener(
            topics = "alerts.notifications.v1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(AlertNotificationEvent event) {

        dispatcher.dispatch(event);
    }
}