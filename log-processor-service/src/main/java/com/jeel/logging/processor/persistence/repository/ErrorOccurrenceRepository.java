package com.jeel.logging.processor.persistence.repository;

import com.jeel.logging.processor.persistence.entity.ErrorOccurrenceEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ErrorOccurrenceRepository
        extends JpaRepository<ErrorOccurrenceEntity, UUID> {
}
