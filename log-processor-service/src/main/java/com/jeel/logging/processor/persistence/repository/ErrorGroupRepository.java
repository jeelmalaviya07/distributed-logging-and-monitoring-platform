package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.api.dto.IssueDetailResponse;
import com.jeel.logging.processor.api.dto.IssueSummaryResponse;
import com.jeel.logging.processor.persistence.entity.ErrorGroupEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ErrorGroupRepository
        extends JpaRepository<ErrorGroupEntity, UUID>,
        JpaSpecificationExecutor<ErrorGroupEntity> {

    Optional<ErrorGroupEntity> findByTenantIdAndFingerprint(
            String tenantId,
            String fingerprint
    );
    @Query("""
       SELECT new com.jeel.logging.processor.api.dto.IssueDetailResponse(
           g.id,
           g.tenantId,
           g.serviceName,
           g.environment,
           g.exceptionType,
           g.exceptionMessage,
           g.severity,
           g.occurrenceCount,
           g.firstSeen,
           g.lastSeen,
           g.resolved
       )
       FROM ErrorGroupEntity g
       WHERE g.id = :id
       """)
    IssueDetailResponse findIssueDetailById(UUID id);

    @Query("""
       SELECT new com.jeel.logging.processor.api.dto.IssueSummaryResponse(
           g.id,
           g.tenantId,
           g.serviceName,
           g.environment,
           g.exceptionType,
           g.exceptionMessage,
           g.severity,
           g.occurrenceCount,
           g.firstSeen,
           g.lastSeen,
           g.resolved
       )
       FROM ErrorGroupEntity g
       WHERE g.tenantId = :tenantId
       ORDER BY g.lastSeen DESC
       """)
    Page<IssueSummaryResponse> findIssuesByTenant(
            String tenantId,
            Pageable pageable
    );
}
