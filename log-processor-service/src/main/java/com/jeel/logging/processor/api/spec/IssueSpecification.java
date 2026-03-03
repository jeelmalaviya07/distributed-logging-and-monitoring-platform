package com.jeel.logging.processor.api.spec;

import com.jeel.logging.processor.persistence.entity.ErrorGroupEntity;
import org.springframework.data.jpa.domain.Specification;

import java.time.Instant;

public class IssueSpecification {

    public static Specification<ErrorGroupEntity> hasTenant(String tenantId) {
        return (root, query, cb) ->
                cb.equal(root.get("tenantId"), tenantId);
    }

    public static Specification<ErrorGroupEntity> hasSeverity(String severity) {
        return (root, query, cb) ->
                severity == null ? null :
                        cb.equal(root.get("severity"), severity);
    }

    public static Specification<ErrorGroupEntity> hasResolved(Boolean resolved) {
        return (root, query, cb) ->
                resolved == null ? null :
                        cb.equal(root.get("resolved"), resolved);
    }

    public static Specification<ErrorGroupEntity> hasService(String serviceName) {
        return (root, query, cb) ->
                serviceName == null ? null :
                        cb.equal(root.get("serviceName"), serviceName);
    }

    public static Specification<ErrorGroupEntity> hasEnvironment(String environment) {
        return (root, query, cb) ->
                environment == null ? null :
                        cb.equal(root.get("environment"), environment);
    }

    public static Specification<ErrorGroupEntity> lastSeenAfter(Instant from) {
        return (root, query, cb) ->
                from == null ? null :
                        cb.greaterThanOrEqualTo(root.get("lastSeen"), from);
    }

    public static Specification<ErrorGroupEntity> lastSeenBefore(Instant to) {
        return (root, query, cb) ->
                to == null ? null :
                        cb.lessThanOrEqualTo(root.get("lastSeen"), to);
    }
}