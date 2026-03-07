package com.jeel.logging.ingestion.obs.alert;

import com.jeel.logging.ingestion.obs.redis.RedisKeyFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Service
public class AlertBucketService {

    private final StringRedisTemplate redisTemplate;

    private final int bucketSizeSeconds = 10;
    private final int windowSeconds = 300;

    public AlertBucketService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void increment(
            String tenantId,
            String errorGroupId
    ) {

        long epoch = Instant.now().getEpochSecond();
        long bucket = epoch / bucketSizeSeconds;

        String key = RedisKeyFactory.alertBucketKey(
                tenantId,
                errorGroupId,
                bucket
        );

        redisTemplate.opsForValue().increment(key);
        redisTemplate.expire(
                key,
                windowSeconds + bucketSizeSeconds,
                TimeUnit.SECONDS
        );
    }
}