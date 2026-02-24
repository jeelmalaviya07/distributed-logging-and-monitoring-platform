package com.jeel.logging.processor.api;

import com.jeel.logging.processor.api.dto.IssueDetailResponse;
import com.jeel.logging.processor.api.dto.IssueSummaryResponse;
import com.jeel.logging.processor.api.dto.OccurrenceResponse;
import com.jeel.logging.processor.persistence.repository.ErrorGroupRepository;

import com.jeel.logging.processor.persistence.repository.ErrorOccurrenceRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/issues")
public class IssueQueryController {

    private final ErrorGroupRepository errorGroupRepository;
    private final ErrorOccurrenceRepository errorOccurrenceRepository;

    public IssueQueryController(
            ErrorGroupRepository errorGroupRepository,
            ErrorOccurrenceRepository errorOccurrenceRepository
    ) {
        this.errorGroupRepository = errorGroupRepository;
        this.errorOccurrenceRepository = errorOccurrenceRepository;
    }

    @GetMapping("/{id}")
    public IssueDetailResponse getIssue(
            @PathVariable UUID id
    ) {
        return errorGroupRepository.findIssueDetailById(id);
    }

    @GetMapping("/{id}/occurrences")
    public Page<OccurrenceResponse> listOccurrences(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return errorOccurrenceRepository.findOccurrencesByGroupId(
                id,
                PageRequest.of(page, size)
        );
    }

    @GetMapping
    public Page<IssueSummaryResponse> listIssues(
            @RequestParam String tenantId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return errorGroupRepository.findIssuesByTenant(
                tenantId,
                PageRequest.of(page, size)
        );
    }
}