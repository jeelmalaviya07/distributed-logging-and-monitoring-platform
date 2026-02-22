package com.jeel.logging.processor.persistence.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "retry_jobs",
        indexes = {
                @Index(name = "idx_retry_next_retry_at", columnList = "nextRetryAt"),
                @Index(name = "idx_retry_status", columnList = "status")
        }
)
public class RetryJobEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String eventId;

    @Column(nullable = false)
    private String tenantId;

    @Column(nullable = false)
    private int attempt;

    @Column(nullable = false)
    private Instant nextRetryAt;

    @Column(nullable = false)
    private String status; // PENDING, PROCESSING

    @Column(length = 4000)
    private String lastError;

    @Lob
    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    public RetryJobEntity() {
    }

    public static RetryJobEntity create(
            String eventId,
            String tenantId,
            String payload,
            int attempt,
            Instant nextRetryAt
    ) {
        RetryJobEntity job = new RetryJobEntity();
        job.id = UUID.randomUUID();
        job.eventId = eventId;
        job.tenantId = tenantId;
        job.payload = payload;
        job.attempt = attempt;
        job.nextRetryAt = nextRetryAt;
        job.status = "PENDING";
        job.createdAt = Instant.now();
        job.updatedAt = Instant.now();
        return job;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public Instant getNextRetryAt() {
        return nextRetryAt;
    }

    public void setNextRetryAt(Instant nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}