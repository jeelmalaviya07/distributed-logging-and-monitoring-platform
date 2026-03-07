package com.jeel.logging.processor.consumer;

import com.jeel.logging.common.events.LogEvent;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.common.events.NormalizedLogEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RawLogEventConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public RawLogEventConsumer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "logs.raw.v1",
            containerFactory = "rawKafkaListenerContainerFactory"
    )
    public void consume(
            LogIngestedEvent batchEvent,
            Acknowledgment ack
    ) {

        try {

            for (LogEvent log : batchEvent.getLogs()) {

                NormalizedLogEvent normalized = new NormalizedLogEvent();
                normalized.setTenantId(batchEvent.getTenantId());
                normalized.setServiceName(batchEvent.getServiceName());
                normalized.setEnvironment(batchEvent.getEnvironment());
                normalized.setLogLevel(log.getLevel());
                normalized.setMessage(log.getMessage());
                normalized.setTimestamp(log.getTimestamp());
                normalized.setTraceId(log.getTraceId());
                normalized.setSpanId(log.getSpanId());

                if (log.getException() != null) {
                    normalized.setExceptionType(log.getException().getType());
                    normalized.setExceptionMessage(log.getException().getMessage());
                }

                String fingerprint = generateFingerprint(normalized);
                normalized.setFingerprint(fingerprint);

                kafkaTemplate.send("logs.normalized.v1", normalized.getTenantId(), normalized);
            }

            // 🔥 COMMIT OFFSET
            ack.acknowledge();

        } catch (Exception ex) {
            // Even on failure — do not block partition forever
            ack.acknowledge();
        }
    }

    private String generateFingerprint(NormalizedLogEvent event) {
        String raw =
                event.getTenantId() + "|" +
                        event.getServiceName() + "|" +
                        event.getEnvironment() + "|" +
                        event.getExceptionType() + "|" +
                        event.getExceptionMessage();

        return Integer.toHexString(raw.hashCode());
    }
}