package com.jeel.logging.processor.alert.notification;

import com.jeel.logging.processor.alert.entity.AlertRuleEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EmailNotificationChannel implements NotificationChannel {

    private static final Logger log =
            LoggerFactory.getLogger(EmailNotificationChannel.class);

    @Override
    public void sendFiring(AlertRuleEntity rule, long count) {

        log.warn("📧 EMAIL ALERT FIRING → tenant={} service={} count={}",
                rule.getTenantId(),
                rule.getServiceName(),
                count
        );
    }

    @Override
    public void sendResolved(AlertRuleEntity rule) {

        log.info("📧 EMAIL ALERT RESOLVED → tenant={} service={}",
                rule.getTenantId(),
                rule.getServiceName()
        );
    }
}