package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.persistence.entity.ErrorGroupEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ErrorGroupRepository
        extends JpaRepository<ErrorGroupEntity, UUID> {

    Optional<ErrorGroupEntity> findByTenantIdAndFingerprint(
            String tenantId,
            String fingerprint
    );
}
