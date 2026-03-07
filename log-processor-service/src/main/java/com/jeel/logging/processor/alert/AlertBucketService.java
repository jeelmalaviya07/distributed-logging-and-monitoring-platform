package com.jeel.logging.processor.alert;

import com.jeel.logging.processor.consumer.NormalizedLogStorageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Service
public class AlertBucketService {

    private static final Logger log =
            LoggerFactory.getLogger(AlertBucketService.class);

    private final StringRedisTemplate redisTemplate;

    private final int bucketSizeSeconds = 5;
    private final int windowSeconds = 60;

    public AlertBucketService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void increment(String tenantId, String groupId) {

        long epoch = Instant.now().getEpochSecond();
        long bucket = epoch / bucketSizeSeconds;

        String key = "obs:{" + tenantId + "}:alert:" + groupId + ":" + bucket;

        redisTemplate.opsForValue().increment(key);
        redisTemplate.expire(key, windowSeconds + bucketSizeSeconds, TimeUnit.SECONDS);
    }

    public boolean shouldTriggerEvaluation(String tenantId, String groupId) {

        String key = "obs:{" + tenantId + "}:alerteval:" + groupId;

        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(key, "1", Duration.ofSeconds(2));

        log.info("-------------");
        log.info((acquired==Boolean.TRUE)?"true":"false");
        log.info("--------------");

        return Boolean.TRUE.equals(acquired);
    }
}