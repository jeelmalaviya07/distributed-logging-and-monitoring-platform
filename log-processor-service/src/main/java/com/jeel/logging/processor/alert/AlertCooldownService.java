package com.jeel.logging.processor.alert;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class AlertCooldownService {

    private final StringRedisTemplate redisTemplate;

    private static final int COOLDOWN_SECONDS = 300; // 5 minutes

    public AlertCooldownService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean canPublish(String tenantId, String groupId) {

        String key = "obs:{" + tenantId + "}:alertcooldown:" + groupId;

        return !(redisTemplate.hasKey(key));
    }

        public boolean tryAcquire(String tenantId, String groupId) {

        String key = "obs:{" + tenantId + "}:alertcooldown:" + groupId;


        Boolean success = redisTemplate.opsForValue()
                .setIfAbsent(key, "1", COOLDOWN_SECONDS, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(success);
    }
}
