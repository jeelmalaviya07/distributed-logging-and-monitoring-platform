package com.jeel.logging.processor.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.processor.persistence.entity.RetryJobEntity;
import com.jeel.logging.processor.persistence.repository.RetryJobRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class RetryJobService {

    private final RetryJobRepository repository;
    private final ObjectMapper objectMapper;
    private final ExponentialBackoffStrategy backoffStrategy;

    public RetryJobService(
            RetryJobRepository repository,
            ObjectMapper objectMapper,
            ExponentialBackoffStrategy backoffStrategy
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.backoffStrategy = backoffStrategy;
    }

    public void scheduleRetry(LogIngestedEvent event, int attempt, String error) {

        try {
            String payload = objectMapper.writeValueAsString(event);

            RetryJobEntity job = RetryJobEntity.create(
                    event.getTenantId() + ":" + event.getRequestId(),
                    event.getTenantId(),
                    payload,
                    attempt,
                    backoffStrategy.computeNextRetry(attempt)
            );

            job.setLastError(error);

            repository.save(job);

        } catch (Exception e) {
            throw new RuntimeException("Failed to schedule retry job", e);
        }
    }
}