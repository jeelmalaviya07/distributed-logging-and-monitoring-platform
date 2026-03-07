package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.alert.AlertHistory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AlertHistoryRepository extends JpaRepository<AlertHistory, Long> {
}