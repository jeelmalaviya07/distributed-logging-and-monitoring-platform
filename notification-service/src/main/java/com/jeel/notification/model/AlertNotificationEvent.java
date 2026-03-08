package com.jeel.notification.model;

import java.time.Instant;

public class AlertNotificationEvent {

    private String tenantId;
    private String serviceName;
    private String groupId;

    private String state;

    private long triggeredCount;
    private Instant timestamp;

    public AlertNotificationEvent() {}

    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public long getTriggeredCount() { return triggeredCount; }
    public void setTriggeredCount(long triggeredCount) { this.triggeredCount = triggeredCount; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}