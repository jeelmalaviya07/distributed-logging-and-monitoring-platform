package com.jeel.logging.processor.metrics;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConsumerMetrics {

    private final AtomicInteger successCount = new AtomicInteger();
    private final AtomicInteger retryCount = new AtomicInteger();
    private final AtomicInteger dlqCount = new AtomicInteger();

    public void markSuccess() {
        successCount.incrementAndGet();
    }

    public void markRetry() {
        retryCount.incrementAndGet();
    }

    public void markDlq() {
        dlqCount.incrementAndGet();
    }

    public int getSuccessCount() {
        return successCount.get();
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public int getDlqCount() {
        return dlqCount.get();
    }
}
