package com.jeel.logging.processor.consumer;

import com.jeel.logging.common.events.LogEvent;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.processor.metrics.ConsumerMetrics;
import com.jeel.logging.processor.persistence.entity.ErrorGroupEntity;
import com.jeel.logging.processor.persistence.entity.ErrorOccurrenceEntity;
import com.jeel.logging.processor.persistence.entity.ProcessedEventEntity;
import com.jeel.logging.processor.persistence.repository.ErrorGroupRepository;
import com.jeel.logging.processor.persistence.repository.ErrorOccurrenceRepository;
import com.jeel.logging.processor.persistence.repository.ProcessedEventRepository;
import com.jeel.logging.processor.retry.RetryDecider;
import com.jeel.logging.processor.retry.RetryJobService;
import com.jeel.logging.processor.validation.LogIngestedEventValidator;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class LogIngestedEventConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(LogIngestedEventConsumer.class);

    private final ProcessedEventRepository processedEventRepository;
    private final LogIngestedEventValidator validator;
    private final ErrorGroupRepository errorGroupRepository;
    private final ErrorOccurrenceRepository errorOccurrenceRepository;
    private final DlqKafkaPublisher dlqKafkaPublisher;
    private final ConsumerMetrics consumerMetrics;
    private final RetryDecider retryDecider;
    private final RetryJobService retryJobService;

    public LogIngestedEventConsumer(
            ProcessedEventRepository processedEventRepository,
            LogIngestedEventValidator validator,
            ErrorGroupRepository errorGroupRepository,
            ErrorOccurrenceRepository errorOccurrenceRepository,
            DlqKafkaPublisher dlqKafkaPublisher,
            ConsumerMetrics consumerMetrics,
            RetryDecider retryDecider,
            RetryJobService retryJobService
    ) {
        this.processedEventRepository = processedEventRepository;
        this.validator = validator;
        this.errorGroupRepository = errorGroupRepository;
        this.errorOccurrenceRepository = errorOccurrenceRepository;
        this.dlqKafkaPublisher = dlqKafkaPublisher;
        this.consumerMetrics = consumerMetrics;
        this.retryDecider = retryDecider;
        this.retryJobService = retryJobService;
    }

    @KafkaListener(
            topics = {"logs.ingested.v1"},
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            LogIngestedEvent event,
            Acknowledgment ack
    ) {

        String safeTenantId = event.getTenantId() != null
                ? event.getTenantId()
                : "UNKNOWN";

        String eventId = safeTenantId + ":" + event.getRequestId();

        try {

            validator.validate(event);

            if (processedEventRepository.existsById(eventId)) {
                log.warn("Duplicate event ignored | eventId={}", eventId);
                ack.acknowledge();
                return;
            }

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

            log.error("Processing failed | eventId={}", eventId, ex);

            boolean retryable = retryDecider.isRetryable(ex);

            if (!retryable) {

                dlqKafkaPublisher.publishToDlq(
                        event,
                        "NON_RETRYABLE: " + ex.getMessage(),
                        0
                );

                consumerMetrics.markDlq();
                ack.acknowledge();
                return;
            }

            // Durable retry (DB-based)
            retryJobService.scheduleRetry(
                    event,
                    1,
                    ex.getMessage()
            );

            consumerMetrics.markRetry();
            ack.acknowledge();
        }
    }

    private void processSingleLog(LogIngestedEvent event, LogEvent logEvent) {

        if (!"ERROR".equalsIgnoreCase(logEvent.getLevel())) {
            return;
        }

        String fingerprint = generateFingerprint(event, logEvent);
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