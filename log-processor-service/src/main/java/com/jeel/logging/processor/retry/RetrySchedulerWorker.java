package com.jeel.logging.processor.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.processor.config.RetryConfig;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;
import com.jeel.logging.processor.metrics.ConsumerMetrics;
import com.jeel.logging.processor.persistence.entity.RetryJobEntity;
import com.jeel.logging.processor.persistence.repository.RetryJobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class RetrySchedulerWorker {

    private static final Logger log =
            LoggerFactory.getLogger(RetrySchedulerWorker.class);

    private final RetryJobRepository repository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, LogIngestedEvent> kafkaTemplate;
    private final ExponentialBackoffStrategy backoffStrategy;
    private final RetryConfig retryConfig;
    private final DlqKafkaPublisher dlqKafkaPublisher;
    private final ConsumerMetrics consumerMetrics;

    public RetrySchedulerWorker(
            RetryJobRepository repository,
            ObjectMapper objectMapper,
            KafkaTemplate kafkaTemplate,
            ExponentialBackoffStrategy exponentialBackoffStrategy,
            RetryConfig retryConfig,
            DlqKafkaPublisher dlqKafkaPublisher,
            ConsumerMetrics consumerMetrics
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.backoffStrategy = exponentialBackoffStrategy;
        this.retryConfig = retryConfig;
        this.dlqKafkaPublisher = dlqKafkaPublisher;
        this.consumerMetrics = consumerMetrics;
    }

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processRetries() {

        List<RetryJobEntity> dueJobs =
                repository.lockAndFetchDueJobs(Instant.now());

        for (RetryJobEntity job : dueJobs) {

            try {
                LogIngestedEvent event =
                        objectMapper.readValue(
                                job.getPayload(),
                                LogIngestedEvent.class
                        );

                kafkaTemplate.send(
                        "logs.ingested.v1",
                        event.getTenantId(),
                        event
                );

                repository.delete(job);
                consumerMetrics.markSuccess();

            } catch (Exception ex) {

                int nextAttempt = job.getAttempt() + 1;

                if (nextAttempt > retryConfig.getMaxAttempts()) {

                    try {
                        LogIngestedEvent event =
                                objectMapper.readValue(
                                        job.getPayload(),
                                        LogIngestedEvent.class
                                );

                        dlqKafkaPublisher.publishToDlq(
                                event,
                                "MAX_RETRIES_EXCEEDED: " + ex.getMessage(),
                                job.getAttempt()
                        );

                        repository.delete(job);
                        consumerMetrics.markDlq();

                    } catch (Exception dlqEx) {
                        // Critical failure, keep job for next cycle
                    }

                } else {

                    job.setAttempt(nextAttempt);
                    job.setNextRetryAt(
                            backoffStrategy.computeNextRetry(nextAttempt)
                    );
                    job.setLastError(ex.getMessage());
                    job.setUpdatedAt(Instant.now());

                    repository.save(job);
                    consumerMetrics.markRetry();
                }
            }
        }
    }
}