package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.persistence.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEventEntity, String> {
}

