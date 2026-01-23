package com.jeel.logging.ingestion.port;

import com.jeel.logging.common.events.LogIngestedEvent;

public interface LogEventPublisher {
    void publish(LogIngestedEvent event);
}
