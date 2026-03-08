package com.jeel.notification.channel;

import com.jeel.notification.entity.AlertRule;
import com.jeel.notification.model.AlertNotificationEvent;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Component
public class EmailSender {

    private final JavaMailSender mailSender;

    public EmailSender(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    public void send(AlertNotificationEvent event, AlertRule rule) {

        if (rule.getEmailTo() == null || rule.getEmailTo().isEmpty()) {
            return;
        }

        SimpleMailMessage message = new SimpleMailMessage();

        message.setTo(rule.getEmailTo());

        message.setSubject(
                "🚨 ALERT: " + event.getServiceName()
        );

        message.setText(
                "Alert Triggered\n\n" +
                        "Tenant: " + event.getTenantId() + "\n" +
                        "Service: " + event.getServiceName() + "\n" +
                        "Group: " + event.getGroupId() + "\n" +
                        "Error Count: " + event.getTriggeredCount() + "\n" +
                        "Time: " + event.getTimestamp()
        );

        mailSender.send(message);
    }
}