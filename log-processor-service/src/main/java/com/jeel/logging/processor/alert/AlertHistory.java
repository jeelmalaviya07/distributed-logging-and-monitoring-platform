package com.jeel.logging.processor.alert;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "alert_history")
public class AlertHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "rule_id", nullable = false)
    private Long ruleId;

    @Column(name = "service_name", nullable = false)
    private String serviceName;

    @Column(name = "severity", nullable = false)
    private String severity;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "triggered_at", nullable = false)
    private Instant triggeredAt;

    @Column(name = "triggered_count", nullable = false)
    private Long triggeredCount;

    public AlertHistory() {
    }

    public Long getId() {
        return id;
    }

    public Long getRuleId() {
        return ruleId;
    }

    public void setRuleId(Long ruleId) {
        this.ruleId = ruleId;
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

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Instant getTriggeredAt() {
        return triggeredAt;
    }

    public void setTriggeredAt(Instant triggeredAt) {
        this.triggeredAt = triggeredAt;
    }

    public Long getTriggeredCount() {
        return triggeredCount;
    }

    public void setTriggeredCount(Long triggeredCount) {
        this.triggeredCount = triggeredCount;
    }
}