package com.jeel.logging.processor.consumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jeel.logging.processor.config.RetryConfig;
import com.jeel.logging.processor.metrics.ConsumerMetrics;
import com.jeel.logging.processor.persistence.entity.DlqEventEntity;
import com.jeel.logging.processor.persistence.entity.ErrorGroupEntity;
import com.jeel.logging.processor.persistence.entity.ErrorOccurrenceEntity;
import com.jeel.logging.processor.persistence.entity.ProcessedEventEntity;
import com.jeel.logging.processor.persistence.repository.DlqEventRepository;
import com.jeel.logging.processor.persistence.repository.ErrorGroupRepository;
import com.jeel.logging.processor.persistence.repository.ErrorOccurrenceRepository;
import com.jeel.logging.processor.persistence.repository.ProcessedEventRepository;
import com.jeel.logging.processor.retry.RetryDecider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.jeel.logging.processor.validation.LogIngestedEventValidator;
import com.jeel.logging.processor.kafka.RetryKafkaPublisher;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.common.events.LogEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;

import java.time.Instant;
import java.util.UUID;

@Component
public class LogIngestedEventConsumer {

    private final ProcessedEventRepository processedEventRepository;
    private final LogIngestedEventValidator validator;
    private final RetryKafkaPublisher retryKafkaPublisher;
    private final DlqKafkaPublisher dlqKafkaPublisher;
    private final DlqEventRepository dlqEventRepository;
    private final ObjectMapper objectMapper;
    private final ErrorGroupRepository errorGroupRepository;
    private final ErrorOccurrenceRepository errorOccurrenceRepository;
    private final RetryConfig retryConfig;
    private final RetryDecider retryDecider;
    private final ConsumerMetrics consumerMetrics;

    public LogIngestedEventConsumer(
            ProcessedEventRepository processedEventRepository,
            LogIngestedEventValidator validator,
            RetryKafkaPublisher retryKafkaPublisher,
            DlqKafkaPublisher dlqKafkaPublisher,
            DlqEventRepository dlqEventRepository,
            ObjectMapper objectMapper,
            ErrorGroupRepository errorGroupRepository,
            ErrorOccurrenceRepository errorOccurrenceRepository,
            RetryConfig retryConfig,
            RetryDecider retryDecider,
            ConsumerMetrics consumerMetrics
    ) {
        this.processedEventRepository = processedEventRepository;
        this.validator = validator;
        this.retryKafkaPublisher = retryKafkaPublisher;
        this.dlqKafkaPublisher = dlqKafkaPublisher;
        this.dlqEventRepository = dlqEventRepository;
        this.objectMapper = objectMapper;
        this.errorGroupRepository = errorGroupRepository;
        this.errorOccurrenceRepository = errorOccurrenceRepository;
        this.retryConfig = retryConfig;
        this.retryDecider = retryDecider;
        this.consumerMetrics = consumerMetrics;
    }

    private static final Logger log =
            LoggerFactory.getLogger(LogIngestedEventConsumer.class);

    @KafkaListener(
            topics = {"logs.ingested.v1", "logs.retry.v1"},
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            LogIngestedEvent event,
            Acknowledgment ack,
            ConsumerRecord<String, LogIngestedEvent> record
    )  {

        int retryCount = RetryKafkaPublisher.extractRetryCount(record.headers());

        String safeTenantId = event.getTenantId() != null
                ? event.getTenantId()
                : "UNKNOWN";

        String eventId = safeTenantId + ":" + event.getRequestId();

        try {

            validator.validate(event);

            if (processedEventRepository.existsById(eventId)) {
                log.warn("⚠️ Duplicate event ignored | eventId={}", eventId);
                ack.acknowledge();
                return;
            }

            log.info("Processing event | tenant={} | service={} | logs={}",
                    event.getTenantId(),
                    event.getServiceName(),
                    event.getLogs().size());

            for (LogEvent logEvent : event.getLogs()) {
                processSingleLog(event, logEvent);
            }

            ProcessedEventEntity pe = new ProcessedEventEntity();
            pe.setEventId(eventId);
            pe.setProcessedAt(Instant.now());
            processedEventRepository.save(pe);
            consumerMetrics.markSuccess();
            ack.acknowledge();

        } catch (Exception ex) {

            log.error("Processing failed | eventId={} | retryCount={}",
                    eventId,
                    retryCount,
                    ex
            );

            // ✅ Check if retryable
            boolean retryable = retryDecider.isRetryable(ex);

            if (!retryable) {

                log.error("Non-retryable failure → Direct DLQ | eventId={}", eventId);
                consumerMetrics.markDlq();
                dlqKafkaPublisher.publishToDlq(
                        event,
                        "NON_RETRYABLE: " + ex.getMessage(),
                        retryCount
                );

                ack.acknowledge();
                return;
            }

            // ✅ Retry if allowed
            int nextRetryCount = retryCount + 1;

            if (nextRetryCount <= retryConfig.getMaxAttempts()) {

                log.warn("Retrying event | eventId={} | attempt={}",
                        eventId,
                        nextRetryCount
                );

                try {
                    Thread.sleep(retryConfig.getBackoffMs() * nextRetryCount);
                } catch (InterruptedException ignored) {}

                consumerMetrics.markRetry();
                retryKafkaPublisher.publishWithRetryHeader(event, nextRetryCount);
                ack.acknowledge();

            } else {

                log.error("Max retries exceeded → DLQ | eventId={}", eventId);

                consumerMetrics.markDlq();
                dlqKafkaPublisher.publishToDlq(
                        event,
                        "MAX_RETRIES_EXCEEDED: " + ex.getMessage(),
                        retryCount
                );

                ack.acknowledge();
            }
        }
    }


    private void processSingleLog(LogIngestedEvent event, LogEvent logEvent) {

        if ("ERROR".equalsIgnoreCase(logEvent.getLevel())) {

            String fingerprint = generateFingerprint(event, logEvent);

            log.warn(
                    "ERROR detected | service={} | fingerprint={}",
                    event.getServiceName(),
                    fingerprint
            );

            Instant now = Instant.now();

            ErrorGroupEntity group =
                    errorGroupRepository
                            .findByTenantIdAndFingerprint(
                                    event.getTenantId(),
                                    fingerprint
                            )
                            .orElseGet(() -> {
                                ErrorGroupEntity g = new ErrorGroupEntity();
                                g.setId(UUID.randomUUID());
                                g.setTenantId(event.getTenantId());
                                g.setServiceName(event.getServiceName());
                                g.setEnvironment(event.getEnvironment());
                                g.setFingerprint(fingerprint);
                                g.setExceptionType(
                                        logEvent.getException() != null
                                                ? logEvent.getException().getType()
                                                : null
                                );
                                g.setExceptionMessage(logEvent.getMessage());
                                g.setSeverity(logEvent.getLevel());
                                g.setFirstSeen(now);
                                g.setLastSeen(now);
                                g.setOccurrenceCount(0);
                                g.setResolved(false);
                                g.setCreatedAt(now);
                                g.setUpdatedAt(now);
                                return errorGroupRepository.save(g);
                            });

            group.setOccurrenceCount(group.getOccurrenceCount() + 1);
            group.setLastSeen(now);
            group.setUpdatedAt(now);

            errorGroupRepository.save(group);

            ErrorOccurrenceEntity occ = new ErrorOccurrenceEntity();

            occ.setId(UUID.randomUUID());
            occ.setTenantId(event.getTenantId());
            occ.setErrorGroup(group);
            occ.setRequestId(event.getRequestId());
            occ.setServiceName(event.getServiceName());
            occ.setEnvironment(event.getEnvironment());
            occ.setTimestamp(logEvent.getTimestamp());
            occ.setLogLevel(logEvent.getLevel());
            occ.setMessage(logEvent.getMessage());
            occ.setTraceId(logEvent.getTraceId());
            occ.setSpanId(logEvent.getSpanId());
            occ.setCreatedAt(now);

            errorOccurrenceRepository.save(occ);

        }
    }

    private String generateFingerprint(LogIngestedEvent batch, LogEvent logEvent) {

        String exceptionType =
                logEvent.getException() != null
                        ? logEvent.getException().getType()
                        : "NO_EXCEPTION";

        String exceptionMessage =
                logEvent.getException() != null
                        ? logEvent.getException().getMessage()
                        : logEvent.getMessage();

        String raw =
                batch.getTenantId() + "|" +
                        batch.getServiceName() + "|" +
                        batch.getEnvironment() + "|" +
                        exceptionType + "|" +
                        exceptionMessage;

        return Integer.toHexString(raw.hashCode());
    }
}
