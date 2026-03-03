package com.jeel.logging.processor.alert.repository;

import com.jeel.logging.processor.alert.entity.AlertRuleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface AlertRuleRepository
        extends JpaRepository<AlertRuleEntity, UUID> {

    List<AlertRuleEntity> findByEnabledTrue();
}