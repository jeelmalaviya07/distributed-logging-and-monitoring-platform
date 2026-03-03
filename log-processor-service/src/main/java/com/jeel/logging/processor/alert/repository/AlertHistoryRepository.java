package com.jeel.logging.processor.alert.repository;

import com.jeel.logging.processor.alert.entity.AlertHistoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AlertHistoryRepository
        extends JpaRepository<AlertHistoryEntity, UUID> {
}