package com.jeel.notification.channel;

import com.jeel.notification.entity.AlertRule;
import com.jeel.notification.model.AlertNotificationEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Component
public class SlackSender {

    private final RestTemplate restTemplate = new RestTemplate();

    public void send(AlertNotificationEvent event, AlertRule rule) {

        if (rule.getSlackWebhook() == null || rule.getSlackWebhook().isEmpty()) {
            return;
        }

        String message =
                "🚨 ALERT FIRED\n" +
                        "Tenant: " + event.getTenantId() + "\n" +
                        "Service: " + event.getServiceName() + "\n" +
                        "Errors: " + event.getTriggeredCount();

        restTemplate.postForObject(
                rule.getSlackWebhook(),
                Map.of("text", message),
                String.class
        );
    }
}