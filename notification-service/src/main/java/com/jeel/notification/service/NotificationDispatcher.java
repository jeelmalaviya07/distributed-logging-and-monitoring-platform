package com.jeel.notification.service;

import com.jeel.notification.channel.EmailSender;
import com.jeel.notification.channel.SlackSender;
import com.jeel.notification.entity.AlertRule;
import com.jeel.notification.model.AlertNotificationEvent;
import com.jeel.notification.repository.AlertRuleRepository;
import org.springframework.stereotype.Service;

@Service
public class NotificationDispatcher {

    private final AlertRuleRepository ruleRepository;
    private final EmailSender emailSender;
    private final SlackSender slackSender;

    public NotificationDispatcher(
            AlertRuleRepository ruleRepository,
            EmailSender emailSender,
            SlackSender slackSender
    ) {
        this.ruleRepository = ruleRepository;
        this.emailSender = emailSender;
        this.slackSender = slackSender;
    }

    public void dispatch(AlertNotificationEvent event) {

        AlertRule rule =
                ruleRepository
                        .findByTenantIdAndServiceNameAndEnabledTrue(
                                event.getTenantId(),
                                event.getServiceName()
                        )
                        .orElse(null);

        if (rule == null) {
            return;
        }

        emailSender.send(event, rule);
//        slackSender.send(event, rule);
    }
}