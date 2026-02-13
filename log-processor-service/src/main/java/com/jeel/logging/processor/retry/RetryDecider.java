package com.jeel.logging.processor.retry;

import org.springframework.stereotype.Component;

import java.sql.SQLTransientConnectionException;

@Component
public class RetryDecider {

    /**
     * Decide if an exception should be retried or not.
     */
    public boolean isRetryable(Exception ex) {

        // ❌ Permanent failures → Never retry
        if (isPermanent(ex)) {
            return false;
        }

        // ✅ Temporary failures → Retry
        if (isTemporary(ex)) {
            return true;
        }

        // Default fallback → retry few times
        return true;
    }

    private boolean isPermanent(Exception ex) {

        // Validation errors will never succeed
        if (ex instanceof IllegalArgumentException) {
            return true;
        }

        // Missing tenant is permanent
        if (ex.getMessage() != null &&
                ex.getMessage().contains("tenantId")) {
            return true;
        }

        return false;
    }

    private boolean isTemporary(Exception ex) {

        // DB transient connection failures
        if (ex instanceof SQLTransientConnectionException) {
            return true;
        }

        String msg = ex.getMessage() != null
                ? ex.getMessage().toLowerCase()
                : "";

        // Network / DB recoverable errors
        return msg.contains("connection refused")
                || msg.contains("timeout")
                || msg.contains("closed")
                || msg.contains("could not open connection");
    }
}
