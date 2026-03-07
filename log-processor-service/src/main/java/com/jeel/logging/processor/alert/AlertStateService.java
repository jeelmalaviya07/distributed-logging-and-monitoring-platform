package com.jeel.logging.processor.alert;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class AlertStateService {

    private final StringRedisTemplate redisTemplate;

    public AlertStateService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean isAlertActive(String tenantId, String groupId) {

        String key = "obs:{" + tenantId + "}:alertstate:" + groupId;

        String state = redisTemplate.opsForValue().get(key);

        return "FIRING".equals(state);
    }

    public void setAlertActive(String tenantId, String groupId) {

        String key = "obs:{" + tenantId + "}:alertstate:" + groupId;

        redisTemplate.opsForValue()
                .set(key, "FIRING", Duration.ofSeconds(300));
    }
}