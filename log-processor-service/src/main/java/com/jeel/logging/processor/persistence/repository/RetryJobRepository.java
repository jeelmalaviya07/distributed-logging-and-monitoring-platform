package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.persistence.entity.RetryJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface RetryJobRepository extends JpaRepository<RetryJobEntity, UUID> {

    @Query(value = """
        SELECT *
        FROM retry_jobs
        WHERE status = 'PENDING'
        AND next_retry_at <= :now
        ORDER BY next_retry_at ASC
        LIMIT 50
        FOR UPDATE SKIP LOCKED
        """,
            nativeQuery = true)
    List<RetryJobEntity> lockAndFetchDueJobs(Instant now);
}