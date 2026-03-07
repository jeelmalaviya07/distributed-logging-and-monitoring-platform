package com.jeel.logging.processor.alert;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "alert_rules")
public class AlertRule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "service_name", nullable = false)
    private String serviceName;

    @Column(name = "severity", nullable = false)
    private String severity;

    @Column(name = "threshold_count", nullable = false)
    private Integer thresholdCount;

    @Column(name = "window_minutes", nullable = false)
    private Integer windowMinutes;

    @Column(name = "cooldown_minutes", nullable = false)
    private Integer cooldownMinutes;

    @Column(name = "enabled", nullable = false)
    private Boolean enabled;

    @Column(name = "webhook_url")
    private String webhookUrl;

    @Column(name = "last_triggered_at")
    private Instant lastTriggeredAt;

    @Column(name = "last_resolved_at")
    private Instant lastResolvedAt;

    @Column(name = "currently_firing")
    private Boolean currentlyFiring;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    public AlertRule() {
    }

    public Long getId() {
        return id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getSeverity() {
        return severity;
    }

    public Integer getThresholdCount() {
        return thresholdCount;
    }

    public Integer getWindowMinutes() {
        return windowMinutes;
    }

    public Integer getCooldownMinutes() {
        return cooldownMinutes;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public String getWebhookUrl() {
        return webhookUrl;
    }

    public Instant getLastTriggeredAt() {
        return lastTriggeredAt;
    }

    public Instant getLastResolvedAt() {
        return lastResolvedAt;
    }

    public Boolean getCurrentlyFiring() {
        return currentlyFiring;
    }

    public void setCurrentlyFiring(Boolean currentlyFiring) {
        this.currentlyFiring = currentlyFiring;
    }

    public void setLastTriggeredAt(Instant lastTriggeredAt) {
        this.lastTriggeredAt = lastTriggeredAt;
    }
}