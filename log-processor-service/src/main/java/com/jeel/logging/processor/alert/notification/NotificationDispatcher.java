package com.jeel.logging.processor.alert.notification;

import com.jeel.logging.processor.alert.entity.AlertRuleEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class NotificationDispatcher {

    private final List<NotificationChannel> channels;

    public NotificationDispatcher(List<NotificationChannel> channels) {
        this.channels = channels;
    }

    @Async("notificationExecutor")
    public void notifyFiring(AlertRuleEntity rule, long count) {

        for (NotificationChannel channel : channels) {
            channel.sendFiring(rule, count);
        }
    }

    @Async("notificationExecutor")
    public void notifyResolved(AlertRuleEntity rule) {

        for (NotificationChannel channel : channels) {
            channel.sendResolved(rule);
        }
    }
}