package com.jeel.notification.repository;

import com.jeel.notification.entity.AlertRule;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AlertRuleRepository extends JpaRepository<AlertRule, Long> {

    Optional<AlertRule> findByTenantIdAndServiceNameAndEnabledTrue(
            String tenantId,
            String serviceName
    );
}