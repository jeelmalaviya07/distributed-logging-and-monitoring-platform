package com.jeel.logging.ingestion.service;

import com.jeel.logging.ingestion.dto.LogIngestionRequest;

public interface LogIngestionService {

    int ingest(String tenantId, String requestId, LogIngestionRequest request);
}
