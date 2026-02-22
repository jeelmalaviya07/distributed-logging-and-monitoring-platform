package com.jeel.logging.processor.retry;

import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class ExponentialBackoffStrategy {

    private static final long BASE_DELAY_SECONDS = 10;
    private static final long MAX_DELAY_SECONDS = 300;

    public Instant computeNextRetry(int attempt) {

        long delay = (long) (BASE_DELAY_SECONDS * Math.pow(2, attempt - 1));

        if (delay > MAX_DELAY_SECONDS) {
            delay = MAX_DELAY_SECONDS;
        }

        return Instant.now().plusSeconds(delay);
    }
}