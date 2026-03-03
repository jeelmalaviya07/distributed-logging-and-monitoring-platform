package com.jeel.logging.processor.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class SystemMetrics {

    private final Counter alertFiringCounter;
    private final Counter alertResolvedCounter;

    private final Counter webhookSuccessCounter;
    private final Counter webhookFailureCounter;

    private final Counter logsProcessedSuccessCounter;
    private final Counter logsProcessingFailureCounter;

    private final Counter logsRetryScheduledCounter;
    private final Counter logsRetryExecutedCounter;
    private final Counter logsRetryExhaustedCounter;

    private final Counter logsDlqCounter;

    public SystemMetrics(MeterRegistry registry) {

        // Alert metrics
        this.alertFiringCounter =
                registry.counter("alerts.firing.total");

        this.alertResolvedCounter =
                registry.counter("alerts.resolved.total");

        // Webhook metrics
        this.webhookSuccessCounter =
                registry.counter("webhook.success.total");

        this.webhookFailureCounter =
                registry.counter("webhook.failure.total");

        // Processing metrics
        this.logsProcessedSuccessCounter =
                registry.counter("logs.processed.success.total");

        this.logsProcessingFailureCounter =
                registry.counter("logs.processing.failure.total");

        this.logsRetryScheduledCounter =
                registry.counter("logs.retry.scheduled.total");

        this.logsRetryExecutedCounter =
                registry.counter("logs.retry.executed.total");

        this.logsRetryExhaustedCounter =
                registry.counter("logs.retry.exhausted.total");

        this.logsDlqCounter =
                registry.counter("logs.dlq.total");
    }

    // Alert
    public void incrementAlertFiring() {
        alertFiringCounter.increment();
    }

    public void incrementAlertResolved() {
        alertResolvedCounter.increment();
    }

    // Webhook
    public void incrementWebhookSuccess() {
        webhookSuccessCounter.increment();
    }

    public void incrementWebhookFailure() {
        webhookFailureCounter.increment();
    }

    // Processing
    public void incrementProcessingSuccess() {
        logsProcessedSuccessCounter.increment();
    }

    public void incrementProcessingFailure() {
        logsProcessingFailureCounter.increment();
    }

    public void incrementRetryScheduled() {
        logsRetryScheduledCounter.increment();
    }

    public void incrementRetryExecuted() {
        logsRetryExecutedCounter.increment();
    }

    public void incrementRetryExhausted() {
        logsRetryExhaustedCounter.increment();
    }

    public void incrementDlq() {
        logsDlqCounter.increment();
    }
}