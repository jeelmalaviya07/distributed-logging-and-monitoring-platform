package com.jeel.logging.processor.persistence.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "error_occurrences")
public class ErrorOccurrenceEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "error_group_id", nullable = false)
    private ErrorGroupEntity errorGroup;

    @Column(name = "request_id", nullable = false, length = 128)
    private String requestId;

    @Column(name = "service_name", nullable = false, length = 128)
    private String serviceName;

    @Column(name = "environment", nullable = false, length = 64)
    private String environment;

    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;

    @Column(name = "log_level", length = 16)
    private String logLevel;

    @Column(name = "message")
    private String message;

    @Column(name = "trace_id", length = 128)
    private String traceId;

    @Column(name = "span_id", length = 128)
    private String spanId;

    @Column(name = "exception_payload", columnDefinition = "jsonb")
    private String exceptionPayload;

    @Column(name = "raw_event_payload", columnDefinition = "jsonb")
    private String rawEventPayload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public ErrorGroupEntity getErrorGroup() {
        return errorGroup;
    }

    public void setErrorGroup(ErrorGroupEntity errorGroup) {
        this.errorGroup = errorGroup;
    }

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

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    public String getExceptionPayload() {
        return exceptionPayload;
    }

    public void setExceptionPayload(String exceptionPayload) {
        this.exceptionPayload = exceptionPayload;
    }

    public String getRawEventPayload() {
        return rawEventPayload;
    }

    public void setRawEventPayload(String rawEventPayload) {
        this.rawEventPayload = rawEventPayload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
// getters & setters
}
