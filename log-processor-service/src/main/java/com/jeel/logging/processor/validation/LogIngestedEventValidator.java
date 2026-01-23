package com.jeel.logging.processor.validation;

import com.jeel.logging.common.events.LogEvent;
import com.jeel.logging.common.events.LogIngestedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class LogIngestedEventValidator {

    public void validate(LogIngestedEvent event) {

        if (event == null) {
            throw new IllegalArgumentException("Event is null");
        }

        if (!StringUtils.hasText(event.getTenantId())) {
            throw new IllegalArgumentException("tenantId is missing");
        }

        if (!StringUtils.hasText(event.getRequestId())) {
            throw new IllegalArgumentException("requestId is missing");
        }

        if (!StringUtils.hasText(event.getServiceName())) {
            throw new IllegalArgumentException("serviceName is missing");
        }

        if (!StringUtils.hasText(event.getEnvironment())) {
            throw new IllegalArgumentException("environment is missing");
        }

        if (event.getLogs() == null || event.getLogs().isEmpty()) {
            throw new IllegalArgumentException("logs list is empty");
        }

        for (LogEvent logEvent : event.getLogs()) {

            if (logEvent == null) {
                throw new IllegalArgumentException("logEvent is null");
            }

            if (logEvent.getLevel() == null) {
                throw new IllegalArgumentException("logEvent.level is null");
            }

            if (!StringUtils.hasText(logEvent.getMessage())) {
                throw new IllegalArgumentException("logEvent.message is missing");
            }

            if (logEvent.getTimestamp() == null) {
                throw new IllegalArgumentException("logEvent.timestamp is null");
            }
        }
    }
}
