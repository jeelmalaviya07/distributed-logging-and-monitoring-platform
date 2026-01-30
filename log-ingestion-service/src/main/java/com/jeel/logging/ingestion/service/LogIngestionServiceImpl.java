package com.jeel.logging.ingestion.service;

import com.jeel.logging.ingestion.dto.LogIngestionRequest;
import com.jeel.logging.common.events.LogIngestedEvent;
import com.jeel.logging.ingestion.port.LogEventPublisher;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class LogIngestionServiceImpl implements LogIngestionService {

    private final LogEventPublisher logEventPublisher;

    //  THIS IS THE MISSING PART
    public LogIngestionServiceImpl(LogEventPublisher logEventPublisher) {
        this.logEventPublisher = logEventPublisher;
    }

    @Override
    public int ingest(String tenantId, String requestId, LogIngestionRequest request) {

        //  Build Kafka event
        LogIngestedEvent event = new LogIngestedEvent();
        event.setTenantId(null);
        //event.setTenantId(tenantId);
        event.setRequestId(requestId);
        event.setServiceName(request.getServiceName());
        event.setEnvironment(request.getEnvironment());
        event.setLogs(request.getLogs());
        event.setIngestedAt(Instant.now());

        //  THIS IS THE LINE THAT WAS MISSING
        logEventPublisher.publish(event);

        // Return count
        return request.getLogs() != null ? request.getLogs().size() : 0;
    }
}
