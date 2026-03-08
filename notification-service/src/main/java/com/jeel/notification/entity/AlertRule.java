package com.jeel.notification.entity;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "alert_rules")
public class AlertRule {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "tenant_id")
    private String tenantId;

    @Column(name = "service_name")
    private String serviceName;

    @Column(name = "severity")
    private String severity;

    @Column(name = "threshold_count")
    private Integer thresholdCount;

    @Column(name = "window_minutes")
    private Integer windowMinutes;

    @Column(name = "cooldown_minutes")
    private Integer cooldownMinutes;

    @Column(name = "enabled")
    private Boolean enabled;

    @Column(name = "currently_firing")
    private Boolean currentlyFiring;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "last_triggered_at")
    private Instant lastTriggeredAt;

    @Column(name = "last_resolved_at")
    private Instant lastResolvedAt;

    @Column(name = "webhook_url")
    private String webhookUrl;

    @Column(name = "email_to")
    private String emailTo;

    @Column(name = "slack_webhook")
    private String slackWebhook;

    public AlertRule() {}

    public Long getId() { return id; }

    public String getTenantId() { return tenantId; }

    public String getServiceName() { return serviceName; }

    public String getSeverity() { return severity; }

    public Integer getThresholdCount() { return thresholdCount; }

    public Integer getWindowMinutes() { return windowMinutes; }

    public Integer getCooldownMinutes() { return cooldownMinutes; }

    public Boolean getEnabled() { return enabled; }

    public Boolean getCurrentlyFiring() { return currentlyFiring; }

    public Instant getCreatedAt() { return createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }

    public Instant getLastTriggeredAt() { return lastTriggeredAt; }

    public Instant getLastResolvedAt() { return lastResolvedAt; }

    public String getWebhookUrl() { return webhookUrl; }

    public String getEmailTo() { return emailTo; }

    public String getSlackWebhook() { return slackWebhook; }
}