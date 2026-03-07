package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.alert.AlertRule;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AlertRuleRepository extends JpaRepository<AlertRule, Long> {

    List<AlertRule> findByTenantIdAndEnabledTrue(String tenantId);

}