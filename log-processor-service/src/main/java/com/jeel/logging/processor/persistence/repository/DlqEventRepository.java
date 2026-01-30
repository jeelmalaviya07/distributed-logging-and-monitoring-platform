package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.persistence.entity.DlqEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface DlqEventRepository
        extends JpaRepository<DlqEventEntity, UUID> {
}

