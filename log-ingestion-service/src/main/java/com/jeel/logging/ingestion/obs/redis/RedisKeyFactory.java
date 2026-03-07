package com.jeel.logging.ingestion.obs.redis;

public final class RedisKeyFactory {

    private static final String PREFIX = "obs";

    private RedisKeyFactory() {}

    public static String rateLimitKey(String tenantId) {
        return PREFIX + ":{" + tenantId + "}:rate:bucket";
    }

    public static String alertBucketKey(
            String tenantId,
            String errorGroupId,
            long bucket
    ) {
        return PREFIX + ":{" + tenantId + "}:alert:"
                + errorGroupId + ":" + bucket;
    }

    public static String alertStateKey(
            String tenantId,
            String errorGroupId
    ) {
        return PREFIX + ":{" + tenantId + "}:alert:state:"
                + errorGroupId;
    }
}