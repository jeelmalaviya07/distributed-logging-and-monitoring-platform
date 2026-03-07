package com.jeel.logging.ingestion.controller;

import com.jeel.logging.ingestion.dto.LogIngestionRequest;
import com.jeel.logging.ingestion.dto.LogIngestionResponse;
import com.jeel.logging.ingestion.obs.ratelimit.RateLimiterService;
import com.jeel.logging.ingestion.service.LogIngestionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/logs")
public class LogIngestionController {

    private final LogIngestionService ingestionService;
    private final RateLimiterService rateLimiterService;

    public LogIngestionController(
            LogIngestionService ingestionService,
            RateLimiterService rateLimiterService
    ) {
        this.ingestionService = ingestionService;
        this.rateLimiterService = rateLimiterService;
    }

    @PostMapping
    public ResponseEntity<LogIngestionResponse> ingestLogs(
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestBody LogIngestionRequest request
    ) {

        if (!rateLimiterService.allow(tenantId)) {
            throw new ResponseStatusException(
                    HttpStatus.TOO_MANY_REQUESTS,
                    "Rate limit exceeded"
            );
        }

        String requestId = UUID.randomUUID().toString();

        int count = ingestionService.ingest(tenantId, requestId, request);

        LogIngestionResponse response =
                new LogIngestionResponse("ACCEPTED", count, requestId);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
