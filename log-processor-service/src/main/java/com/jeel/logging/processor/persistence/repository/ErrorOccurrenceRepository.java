package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.api.dto.OccurrenceResponse;
import com.jeel.logging.processor.persistence.entity.ErrorOccurrenceEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ErrorOccurrenceRepository
        extends JpaRepository<ErrorOccurrenceEntity, UUID> {

    @Query("""
       SELECT new com.jeel.logging.processor.api.dto.OccurrenceResponse(
           o.id,
           o.requestId,
           o.serviceName,
           o.environment,
           o.logLevel,
           o.message,
           o.traceId,
           o.spanId,
           o.timestamp
       )
       FROM ErrorOccurrenceEntity o
       WHERE o.errorGroup.id = :groupId
       ORDER BY o.timestamp DESC
       """)
    Page<OccurrenceResponse> findOccurrencesByGroupId(
            UUID groupId,
            Pageable pageable
    );
}
