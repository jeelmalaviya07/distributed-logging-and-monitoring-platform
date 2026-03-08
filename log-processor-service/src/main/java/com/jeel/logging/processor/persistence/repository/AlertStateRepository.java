package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.alert.AlertState;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AlertStateRepository extends JpaRepository<AlertState, Long> {

    Optional<AlertState> findByTenantIdAndServiceNameAndGroupId(
            String tenantId,
            String serviceName,
            String groupId
    );
}