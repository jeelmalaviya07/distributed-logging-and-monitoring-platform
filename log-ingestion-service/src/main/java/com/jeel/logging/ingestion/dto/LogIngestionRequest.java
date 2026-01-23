package com.jeel.logging.ingestion.dto;
import com.jeel.logging.common.events.LogEvent;
import java.util.List;

public class LogIngestionRequest {

    private String serviceName;
    private String environment;
    private List<LogEvent> logs;
    private String requestId;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public List<LogEvent> getLogs() {
        return logs;
    }

    public void setLogs(List<LogEvent> logs) {
        this.logs = logs;
    }
}
