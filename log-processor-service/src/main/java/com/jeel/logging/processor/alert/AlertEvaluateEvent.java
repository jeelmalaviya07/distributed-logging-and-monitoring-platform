package com.jeel.logging.processor.alert;

public class AlertEvaluateEvent {

    private String tenantId;
    private String groupId;
    private String serviceName;

    public AlertEvaluateEvent() {}

    public AlertEvaluateEvent(String tenantId, String groupId) {
        this.tenantId = tenantId;
        this.groupId = groupId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}