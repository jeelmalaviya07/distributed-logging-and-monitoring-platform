package com.jeel.logging.processor.alert.notification;

import com.jeel.logging.processor.alert.config.NotificationConfig;
import com.jeel.logging.processor.alert.entity.AlertRuleEntity;
import com.jeel.logging.processor.metrics.SystemMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

import java.util.HashMap;
import java.util.Map;

@Component
public class WebhookNotificationChannel implements NotificationChannel {

    private static final Logger log =
            LoggerFactory.getLogger(WebhookNotificationChannel.class);

    private final NotificationConfig config;
    private final RestTemplate restTemplate;
    private final SystemMetrics metrics;

    public WebhookNotificationChannel(
            NotificationConfig config,
            SystemMetrics metrics
    ) {
        this.config = config;

        SimpleClientHttpRequestFactory factory =
                new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(config.getTimeoutMs());
        factory.setReadTimeout(config.getTimeoutMs());

        this.restTemplate = new RestTemplate(factory);
        this.metrics = metrics;
    }

    @Override
    public void sendFiring(AlertRuleEntity rule, long count) {
        sendWithRetry(rule, buildPayload(rule, count, "FIRING"));
    }

    @Override
    public void sendResolved(AlertRuleEntity rule) {
        sendWithRetry(rule, buildPayload(rule, 0, "RESOLVED"));
    }

    private Map<String, Object> buildPayload(
            AlertRuleEntity rule,
            long count,
            String status
    ) {

        Map<String, Object> payload = new HashMap<>();
        payload.put("tenantId", rule.getTenantId());
        payload.put("serviceName", rule.getServiceName());
        payload.put("severity", rule.getSeverity());
        payload.put("count", count);
        payload.put("status", status);

        return payload;
    }

    private void sendWithRetry(
            AlertRuleEntity rule,
            Map<String, Object> payload
    ) {

        if (rule.getWebhookUrl() == null ||
                rule.getWebhookUrl().isBlank()) {
            return;
        }

        int attempts = 0;

        while (attempts < config.getMaxAttempts()) {

            try {

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<Map<String, Object>> entity =
                        new HttpEntity<>(payload, headers);

                ResponseEntity<String> response =
                        restTemplate.postForEntity(
                                rule.getWebhookUrl(),
                                entity,
                                String.class
                        );

                if (response.getStatusCode().is2xxSuccessful()) {

                    log.info("🌐 Webhook sent successfully → {}",
                            rule.getWebhookUrl());

                    metrics.incrementWebhookSuccess();
                    return;
                }

            } catch (Exception ex) {

                log.warn("Webhook attempt {} failed",
                        attempts + 1);
            }

            attempts++;

            try {
                Thread.sleep(config.getBackoffMs() * attempts);
            } catch (InterruptedException ignored) {}
        }

        log.error("🚨 Webhook permanently failed after {} attempts → {}",
                config.getMaxAttempts(),
                rule.getWebhookUrl());

        metrics.incrementWebhookFailure();
    }
}