package com.jeel.logging.processor.alert.notification;

import com.jeel.logging.processor.alert.entity.AlertRuleEntity;

public interface NotificationChannel {

    void sendFiring(AlertRuleEntity rule, long count);

    void sendResolved(AlertRuleEntity rule);
}