package com.jeel.logging.processor.retry;

import org.springframework.stereotype.Component;

@Component
public class RetryDecider {

    /**
     * Decide if an exception should be retried or not.
     */
    public boolean isRetryable(Exception ex) {

        // ❌ Validation / bad request → never retry
        if (ex instanceof IllegalArgumentException) {
            return false;
        }

        // ❌ Tenant missing → no retry
        if (ex.getMessage() != null &&
                ex.getMessage().contains("tenantId")) {
            return false;
        }

        // ✅ DB/network temporary errors → retry
        String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";

        if (msg.contains("connection refused") ||
                msg.contains("timeout") ||
                msg.contains("could not open connection")) {
            return true;
        }

        // Default → retry (safe fallback)
        return true;
    }
}
