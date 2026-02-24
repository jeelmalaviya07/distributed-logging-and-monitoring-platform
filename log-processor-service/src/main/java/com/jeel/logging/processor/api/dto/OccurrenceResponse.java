package com.jeel.logging.processor.api.dto;

import java.time.Instant;
import java.util.UUID;

public class OccurrenceResponse {

    private UUID id;
    private String requestId;
    private String serviceName;
    private String environment;
    private String logLevel;
    private String message;
    private String traceId;
    private String spanId;
    private Instant timestamp;

    public OccurrenceResponse(
            UUID id,
            String requestId,
            String serviceName,
            String environment,
            String logLevel,
            String message,
            String traceId,
            String spanId,
            Instant timestamp
    ) {
        this.id = id;
        this.requestId = requestId;
        this.serviceName = serviceName;
        this.environment = environment;
        this.logLevel = logLevel;
        this.message = message;
        this.traceId = traceId;
        this.spanId = spanId;
        this.timestamp = timestamp;
    }

    public UUID getId() { return id; }
    public String getRequestId() { return requestId; }
    public String getServiceName() { return serviceName; }
    public String getEnvironment() { return environment; }
    public String getLogLevel() { return logLevel; }
    public String getMessage() { return message; }
    public String getTraceId() { return traceId; }
    public String getSpanId() { return spanId; }
    public Instant getTimestamp() { return timestamp; }
}