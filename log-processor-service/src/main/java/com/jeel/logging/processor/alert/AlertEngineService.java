package com.jeel.logging.processor.alert;

import com.jeel.logging.processor.persistence.repository.AlertHistoryRepository;
import com.jeel.logging.processor.persistence.repository.AlertRuleRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class AlertEngineService {

    private final AlertRuleRepository ruleRepository;
    private final AlertHistoryRepository historyRepository;
    private final AlertCooldownService cooldownService;
    private final AlertNotificationProducer notificationProducer;

    public AlertEngineService(
            AlertRuleRepository ruleRepository,
            AlertHistoryRepository historyRepository,
            AlertCooldownService cooldownService,
            AlertNotificationProducer notificationProducer) {

        this.ruleRepository = ruleRepository;
        this.historyRepository = historyRepository;
        this.cooldownService = cooldownService;
        this.notificationProducer = notificationProducer;
    }

    public void evaluate(
            String tenantId,
            String serviceName,
            String groupId,
            long count) {

        List<AlertRule> rules =
                ruleRepository.findByTenantIdAndEnabledTrue(tenantId);

        for (AlertRule rule : rules) {

            if (!rule.getServiceName().equals(serviceName)) {
                continue;
            }

            if (count < rule.getThresholdCount()) {
                continue;
            }

            boolean allowed =
                    cooldownService.tryAcquire(tenantId, groupId);

            if (!allowed) {
                return;
            }

            AlertHistory history = new AlertHistory();

            history.setRuleId(rule.getId());
            history.setTenantId(tenantId);
            history.setServiceName(serviceName);
            history.setSeverity(rule.getSeverity());
            history.setTriggeredAt(Instant.now());
            history.setTriggeredCount(count);

            historyRepository.save(history);

            rule.setCurrentlyFiring(true);
            rule.setLastTriggeredAt(Instant.now());

            ruleRepository.save(rule);

            notificationProducer.send(history, tenantId);
        }
    }
}