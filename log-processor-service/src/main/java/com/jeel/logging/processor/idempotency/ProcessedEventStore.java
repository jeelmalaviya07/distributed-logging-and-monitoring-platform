package com.jeel.logging.processor.idempotency;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ProcessedEventStore {

    private final Set<String> processedEventIds =
            ConcurrentHashMap.newKeySet();

    public boolean isProcessed(String eventId) {
        return processedEventIds.contains(eventId);
    }

    public void markProcessed(String eventId) {
        processedEventIds.add(eventId);
    }
}
