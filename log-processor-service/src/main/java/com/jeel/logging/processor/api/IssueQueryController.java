package com.jeel.logging.processor.api;

import com.jeel.logging.processor.api.dto.IssueDetailResponse;
import com.jeel.logging.processor.api.dto.IssueSummaryResponse;
import com.jeel.logging.processor.api.dto.OccurrenceResponse;
import com.jeel.logging.processor.api.spec.IssueSpecification;
import com.jeel.logging.processor.persistence.entity.ErrorGroupEntity;
import com.jeel.logging.processor.persistence.repository.ErrorGroupRepository;

import com.jeel.logging.processor.persistence.repository.ErrorOccurrenceRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
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
    public Page<ErrorGroupEntity> listIssues(
            @RequestParam String tenantId,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) Boolean resolved,
            @RequestParam(required = false) String serviceName,
            @RequestParam(required = false) String environment,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {

        Specification<ErrorGroupEntity> spec =
                IssueSpecification.hasTenant(tenantId)
                        .and(IssueSpecification.hasSeverity(severity))
                        .and(IssueSpecification.hasResolved(resolved))
                        .and(IssueSpecification.hasService(serviceName))
                        .and(IssueSpecification.hasEnvironment(environment))
                        .and(IssueSpecification.lastSeenAfter(from))
                        .and(IssueSpecification.lastSeenBefore(to));

        return errorGroupRepository.findAll(
                spec,
                PageRequest.of(page, size)
        );
    }

    @PatchMapping("/{id}/resolve")
    public void resolveIssue(@PathVariable UUID id) {

        ErrorGroupEntity issue = errorGroupRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Issue not found"));

        issue.setResolved(true);
        issue.setUpdatedAt(Instant.now());

        errorGroupRepository.save(issue);
    }

    @PatchMapping("/{id}/reopen")
    public void reopenIssue(@PathVariable UUID id) {

        ErrorGroupEntity issue = errorGroupRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Issue not found"));

        issue.setResolved(false);
        issue.setUpdatedAt(Instant.now());

        errorGroupRepository.save(issue);
    }
}