package com.jeel.logging.ingestion.obs.ratelimit;
import com.jeel.logging.ingestion.obs.redis.RedisKeyFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;

@Service
public class RateLimiterService {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> script;

    private final long capacity = 20;        // configurable
    private final long refillRate = 5;        // tokens per second

    public RateLimiterService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.script = new DefaultRedisScript<>();
        this.script.setLocation(new ClassPathResource("redis/token_bucket.lua"));
        this.script.setResultType(Long.class);
    }

    public boolean allow(String tenantId) {

        String key = RedisKeyFactory.rateLimitKey(tenantId);
        long now = Instant.now().getEpochSecond();

        Long result = redisTemplate.execute(
                script,
                Collections.singletonList(key),
                String.valueOf(capacity),
                String.valueOf(refillRate),
                String.valueOf(now),
                "1"
        );

        return result != null && result == 1;
    }
}