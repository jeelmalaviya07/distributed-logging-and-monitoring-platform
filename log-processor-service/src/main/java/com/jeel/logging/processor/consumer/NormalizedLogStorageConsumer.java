package com.jeel.logging.processor.consumer;

import com.jeel.logging.common.events.NormalizedLogEvent;
import com.jeel.logging.processor.alert.AlertBucketService;
import com.jeel.logging.processor.alert.AlertCooldownService;
import com.jeel.logging.processor.alert.SlidingWindowAlertService;
import com.jeel.logging.processor.kafka.AlertEvaluationPublisher;
import com.jeel.logging.processor.kafka.DlqKafkaPublisher;
import com.jeel.logging.processor.kafka.RetryKafkaPublisher;
import com.jeel.logging.processor.metrics.ConsumerMetrics;
import com.jeel.logging.processor.metrics.SystemMetrics;
import com.jeel.logging.processor.persistence.entity.ErrorGroupEntity;
import com.jeel.logging.processor.persistence.entity.ErrorOccurrenceEntity;
import com.jeel.logging.processor.persistence.entity.ProcessedEventEntity;
import com.jeel.logging.processor.persistence.repository.ErrorGroupRepository;
import com.jeel.logging.processor.persistence.repository.ErrorOccurrenceRepository;
import com.jeel.logging.processor.persistence.repository.ProcessedEventRepository;
import com.jeel.logging.processor.retry.RetryDecider;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

@Component
public class NormalizedLogStorageConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(NormalizedLogStorageConsumer.class);

    private final ProcessedEventRepository processedEventRepository;
    private final ErrorGroupRepository errorGroupRepository;
    private final ErrorOccurrenceRepository errorOccurrenceRepository;
    private final DlqKafkaPublisher dlqKafkaPublisher;
    private final RetryKafkaPublisher retryKafkaPublisher;
    private final ConsumerMetrics consumerMetrics;
    private final RetryDecider retryDecider;
    private final SystemMetrics metrics;
    private final AlertBucketService alertBucketService;
    private final AlertEvaluationPublisher alertEvaluationPublisher;
    private final SlidingWindowAlertService slidingWindowAlertService;
    private final AlertCooldownService alertCooldownService;

    public NormalizedLogStorageConsumer(
            ProcessedEventRepository processedEventRepository,
            ErrorGroupRepository errorGroupRepository,
            ErrorOccurrenceRepository errorOccurrenceRepository,
            DlqKafkaPublisher dlqKafkaPublisher,
            RetryKafkaPublisher retryKafkaPublisher,
            ConsumerMetrics consumerMetrics,
            RetryDecider retryDecider,
            SystemMetrics metrics,
            AlertBucketService alertBucketService,
            AlertEvaluationPublisher alertEvaluationPublisher,
            SlidingWindowAlertService slidingWindowAlertService,
            AlertCooldownService alertCooldownService
    ) {
        this.processedEventRepository = processedEventRepository;
        this.errorGroupRepository = errorGroupRepository;
        this.errorOccurrenceRepository = errorOccurrenceRepository;
        this.dlqKafkaPublisher = dlqKafkaPublisher;
        this.retryKafkaPublisher = retryKafkaPublisher;
        this.consumerMetrics = consumerMetrics;
        this.retryDecider = retryDecider;
        this.metrics = metrics;
        this.alertBucketService = alertBucketService;
        this.alertEvaluationPublisher = alertEvaluationPublisher;
        this.slidingWindowAlertService = slidingWindowAlertService;
        this.alertCooldownService = alertCooldownService;
    }

    @KafkaListener(
            topics = {"logs.normalized.v1"},
            containerFactory = "normalizedKafkaListenerContainerFactory"
    )
    @Transactional //adding as it saved 1 log before it sent an exception in stress testing
    public void consume(
            ConsumerRecord<String, NormalizedLogEvent> record,
            Acknowledgment ack
    ) {

        NormalizedLogEvent event = record.value();
        String eventId = event.getTenantId() + ":" + event.getEventId();

//        log.info("-----------------------------------");
//        log.info(record.toString());
//        log.info(event.getLogLevel());
//        log.info("======================================");

        try {

            if (processedEventRepository.existsById(eventId)) {
                log.warn("Duplicate event ignored | eventId={}", eventId);
                ack.acknowledge();
                return;
            }

            processSingleNormalizedEvent(event);

            ProcessedEventEntity pe = new ProcessedEventEntity();
            pe.setEventId(eventId);
            pe.setProcessedAt(Instant.now());
            processedEventRepository.save(pe);
            metrics.incrementProcessingSuccess();
            consumerMetrics.markSuccess();
            ack.acknowledge();

        }catch (Exception ex) {

            log.error("Processing failed | eventId={}", eventId, ex);

            int retryCount = extractRetryCount(record);

            if (!retryDecider.isRetryable(ex)) {
                dlqKafkaPublisher.publishToDlq(event, "NON_RETRYABLE: "+ex.getMessage(), retryCount);
                ack.acknowledge();
                return;
            }

            switch (retryCount) {
                case 0:
                    retryKafkaPublisher.publishToRetry(event, 1);
                    break;
                case 1:
                    retryKafkaPublisher.publishToRetry(event, 2);
                    break;
                case 2:
                    retryKafkaPublisher.publishToRetry(event, 3);
                    break;
                default:
                    dlqKafkaPublisher.publishToDlq(event, "RETRY_EXHAUSTED", retryCount);
            }

            metrics.incrementRetryScheduled();
            consumerMetrics.markRetry();
            ack.acknowledge();
        }
    }

    private int extractRetryCount(ConsumerRecord<String, NormalizedLogEvent> record) {

        Header header = record.headers().lastHeader("x-retry-count");

        if (header == null) {
            return 0;
        }

        return Integer.parseInt(
                new String(header.value(), StandardCharsets.UTF_8));
    }

    @Transactional
    private void processSingleNormalizedEvent(NormalizedLogEvent event) {

        if (!"ERROR".equalsIgnoreCase(event.getLogLevel())) {
            return;
        }

        Instant now = Instant.now();

        ErrorGroupEntity group =
                errorGroupRepository
                        .findByTenantIdAndFingerprint(
                                event.getTenantId(),
                                event.getFingerprint()
                        )
                        .orElseGet(() -> {
                            ErrorGroupEntity g = new ErrorGroupEntity();
                            g.setId(UUID.randomUUID());
                            g.setTenantId(event.getTenantId());
                            g.setServiceName(event.getServiceName());
                            g.setEnvironment(event.getEnvironment());
                            g.setFingerprint(event.getFingerprint());
                            g.setExceptionType(event.getExceptionType());
                            g.setExceptionMessage(event.getExceptionMessage());
                            g.setSeverity(event.getLogLevel());
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
        occ.setRequestId(event.getEventId());
        occ.setServiceName(event.getServiceName());
        occ.setEnvironment(event.getEnvironment());
        occ.setTimestamp(event.getTimestamp());
        occ.setLogLevel(event.getLogLevel());
        occ.setMessage(event.getMessage());
        occ.setTraceId(event.getTraceId());
        occ.setSpanId(event.getSpanId());
        occ.setCreatedAt(now);

        errorOccurrenceRepository.save(occ);
        log.info("Normalized Log Event");

        long count = slidingWindowAlertService.recordAndCount(
                event.getTenantId(),
                group.getId().toString()
        );

        if (slidingWindowAlertService.shouldAlert(count)) {

            if (alertBucketService.shouldTriggerEvaluation(
                    event.getTenantId(),
                    group.getId().toString()
            )) {

                if (alertCooldownService.canPublish(
                        event.getTenantId(),
                        group.getId().toString()
                )) {

                    alertEvaluationPublisher.publish(
                            event.getTenantId(),
                            group.getId().toString(),
                            event.getServiceName()
                    );

                }
            }
        }
    }
}