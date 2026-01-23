package com.jeel.logging.common.events;

import java.time.Instant;
import java.util.List;

import com.jeel.logging.common.events.LogEvent;

public class LogIngestedEvent {

    private String tenantId;
    private String requestId;

    private String serviceName;
    private String environment;

    private Instant ingestedAt;

    private List<LogEvent> logs;

    public LogIngestedEvent() {
    }

    public LogIngestedEvent(
            String tenantId,
            String requestId,
            String serviceName,
            String environment,
            Instant ingestedAt,
            List<LogEvent> logs) {
        this.tenantId = tenantId;
        this.requestId = requestId;
        this.serviceName = serviceName;
        this.environment = environment;
        this.ingestedAt = ingestedAt;
        this.logs = logs;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getEnvironment() {
        return environment;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }

    public List<LogEvent> getLogs() {
        return logs;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public void setIngestedAt(Instant ingestedAt) {
        this.ingestedAt = ingestedAt;
    }

    public void setLogs(List<LogEvent> logs) {
        this.logs = logs;
    }
}

