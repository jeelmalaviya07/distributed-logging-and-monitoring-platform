package com.jeel.logging.processor.api.dto;

import java.time.Instant;
import java.util.UUID;

public class IssueSummaryResponse {

    private UUID id;
    private String tenantId;
    private String serviceName;
    private String environment;
    private String exceptionType;
    private String exceptionMessage;
    private String severity;
    private long occurrenceCount;
    private Instant firstSeen;
    private Instant lastSeen;
    private boolean resolved;

    public IssueSummaryResponse(
            UUID id,
            String tenantId,
            String serviceName,
            String environment,
            String exceptionType,
            String exceptionMessage,
            String severity,
            long occurrenceCount,
            Instant firstSeen,
            Instant lastSeen,
            boolean resolved
    ) {
        this.id = id;
        this.tenantId = tenantId;
        this.serviceName = serviceName;
        this.environment = environment;
        this.exceptionType = exceptionType;
        this.exceptionMessage = exceptionMessage;
        this.severity = severity;
        this.occurrenceCount = occurrenceCount;
        this.firstSeen = firstSeen;
        this.lastSeen = lastSeen;
        this.resolved = resolved;
    }

    public UUID getId() { return id; }
    public String getTenantId() { return tenantId; }
    public String getServiceName() { return serviceName; }
    public String getEnvironment() { return environment; }
    public String getExceptionType() { return exceptionType; }
    public String getExceptionMessage() { return exceptionMessage; }
    public String getSeverity() { return severity; }
    public long getOccurrenceCount() { return occurrenceCount; }
    public Instant getFirstSeen() { return firstSeen; }
    public Instant getLastSeen() { return lastSeen; }
    public boolean isResolved() { return resolved; }
}