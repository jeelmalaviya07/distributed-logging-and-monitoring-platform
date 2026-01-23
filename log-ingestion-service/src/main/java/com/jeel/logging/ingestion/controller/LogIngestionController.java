package com.jeel.logging.ingestion.controller;

import com.jeel.logging.ingestion.dto.LogIngestionRequest;
import com.jeel.logging.ingestion.dto.LogIngestionResponse;
import com.jeel.logging.ingestion.service.LogIngestionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/logs")
public class LogIngestionController {

    private final LogIngestionService ingestionService;

    public LogIngestionController(LogIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping
    public ResponseEntity<LogIngestionResponse> ingestLogs(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestBody LogIngestionRequest request
    ) {
        String requestId = UUID.randomUUID().toString();

        int count = ingestionService.ingest(tenantId, requestId, request);

        LogIngestionResponse response =
                new LogIngestionResponse("ACCEPTED", count, requestId);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
