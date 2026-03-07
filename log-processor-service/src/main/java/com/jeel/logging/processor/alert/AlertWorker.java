package com.jeel.logging.processor.alert;

import com.jeel.logging.processor.alert.AlertEvaluateEvent;
import com.jeel.logging.processor.persistence.repository.AlertHistoryRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class AlertWorker {

    private final RedisTemplate<String, String> redisTemplate;
    private final AlertHistoryRepository alertHistoryRepository;

    public AlertWorker(
            RedisTemplate<String, String> redisTemplate,
            AlertHistoryRepository alertHistoryRepository
    ){
        this.redisTemplate = redisTemplate;
        this.alertHistoryRepository = alertHistoryRepository;
    }

    @KafkaListener(
            topics = "alerts.evaluate.v1",
            containerFactory = "alertKafkaListenerContainerFactory"
    )
    public void evaluate(AlertEvaluateEvent event) {

        String tenantId = event.getTenantId();
        String groupId = event.getGroupId();

        // sliding window key
        String windowKey = "obs:{" + tenantId + "}:alertwin:" + groupId;

        // cooldown key
        String cooldownKey = "obs:{" + tenantId + "}:alertcooldown:" + groupId;

        // read sliding window size
        Long count = redisTemplate.opsForZSet().zCard(windowKey);

        if (count == null) {
            return;
        }

        // threshold (temporary hardcoded until alert_rules is wired)
        int threshold = 20;

        // check cooldown
        Boolean cooldownExists = redisTemplate.hasKey(cooldownKey);

        System.out.println("Count: "+count+" "+cooldownExists);

        if (count >= threshold && Boolean.FALSE.equals(cooldownExists)) {

            System.out.println(
                    "ALERT FIRING tenant=" + tenantId
                            + " group=" + groupId
                            + " count=" + count
            );

            // enter COOLDOWN state
            redisTemplate.opsForValue().set(
                    cooldownKey,
                    "1",
                    5,
                    java.util.concurrent.TimeUnit.MINUTES
            );

            // optional: persist alert history
            AlertHistory history = new AlertHistory();
            history.setRuleId(0L);
            history.setTenantId(tenantId);
            history.setServiceName(event.getServiceName());
            history.setSeverity("ERROR");
            history.setTriggeredCount((long) count.intValue());
            history.setTriggeredAt(java.time.Instant.now());

            alertHistoryRepository.save(history);
        }
    }
}