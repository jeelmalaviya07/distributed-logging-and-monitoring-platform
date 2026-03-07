package com.jeel.logging.processor.alert;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;

@Service
public class SlidingWindowAlertService {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> script;

    private static final int WINDOW_SECONDS = 60;
    private static final int THRESHOLD = 50;

    public SlidingWindowAlertService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;

        script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/SlidingWindow.lua"));
        script.setResultType(Long.class);
    }

    public long recordAndCount(String tenantId, String groupId) {

        String key = "obs:{" + tenantId + "}:alertwin:" + groupId;
        long now = Instant.now().getEpochSecond();

        Long count = redisTemplate.execute(
                script,
                Collections.singletonList(key),
                String.valueOf(now),
                String.valueOf(WINDOW_SECONDS)
        );

        return count == null ? 0 : count;
    }

    public boolean shouldAlert(long count) {
        return count >= THRESHOLD;
    }
}