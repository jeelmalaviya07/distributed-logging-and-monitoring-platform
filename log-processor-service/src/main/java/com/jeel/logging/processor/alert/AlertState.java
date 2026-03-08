package com.jeel.logging.processor.alert;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(
        name = "alert_state",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"tenantId","serviceName","groupId"}
        )
)
public class AlertState {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String tenantId;
    private String serviceName;
    private String groupId;

    private String state; // OK / FIRING

    private boolean acknowledged;

    private Long triggeredCount;

    private Instant lastTriggeredAt;
    private Instant createdAt;
    private Instant updatedAt;

    public AlertState() {}

    // getters setters

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public void setAcknowledged(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public Long getTriggeredCount() {
        return triggeredCount;
    }

    public void setTriggeredCount(Long triggeredCount) {
        this.triggeredCount = triggeredCount;
    }

    public Instant getLastTriggeredAt() {
        return lastTriggeredAt;
    }

    public void setLastTriggeredAt(Instant lastTriggeredAt) {
        this.lastTriggeredAt = lastTriggeredAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}