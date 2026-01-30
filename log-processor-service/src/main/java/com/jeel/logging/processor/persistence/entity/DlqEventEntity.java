package com.jeel.logging.processor.persistence.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dlq_events")
public class DlqEventEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "tenant_id", length = 64)
    private String tenantId;

    @Column(name = "request_id", length = 128)
    private String requestId;

    @Column(name = "service_name", length = 128)
    private String serviceName;

    @Column(name = "environment", length = 64)
    private String environment;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "final_retry_count")
    private Integer finalRetryCount;

    @Column(name = "event_payload", columnDefinition = "jsonb")
    private String eventPayload;

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

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public Integer getFinalRetryCount() {
        return finalRetryCount;
    }

    public void setFinalRetryCount(Integer finalRetryCount) {
        this.finalRetryCount = finalRetryCount;
    }

    public String getEventPayload() {
        return eventPayload;
    }

    public void setEventPayload(String eventPayload) {
        this.eventPayload = eventPayload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
// getters & setters
}
