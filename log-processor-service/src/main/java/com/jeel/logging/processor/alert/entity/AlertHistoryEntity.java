package com.jeel.logging.processor.alert.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "alert_history")
public class AlertHistoryEntity {

    @Id
    private UUID id;

    private UUID ruleId;

    private String tenantId;

    private String serviceName;

    private String severity;

    private long triggeredCount;

    private Instant triggeredAt;

    @PrePersist
    public void prePersist() {
        this.id = UUID.randomUUID();
        this.triggeredAt = Instant.now();
    }

    // getters and setters


    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getRuleId() {
        return ruleId;
    }

    public void setRuleId(UUID ruleId) {
        this.ruleId = ruleId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public long getTriggeredCount() {
        return triggeredCount;
    }

    public void setTriggeredCount(long triggeredCount) {
        this.triggeredCount = triggeredCount;
    }

    public Instant getTriggeredAt() {
        return triggeredAt;
    }

    public void setTriggeredAt(Instant triggeredAt) {
        this.triggeredAt = triggeredAt;
    }
}