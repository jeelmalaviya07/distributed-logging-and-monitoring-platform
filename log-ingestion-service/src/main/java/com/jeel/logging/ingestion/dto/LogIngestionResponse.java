package com.jeel.logging.ingestion.dto;

public class LogIngestionResponse {

    private String status;
    private int ingestedCount;
    private String requestId;

    public LogIngestionResponse(String status, int ingestedCount, String requestId) {
        this.status = status;
        this.ingestedCount = ingestedCount;
        this.requestId = requestId;
    }

    public String getStatus() {
        return status;
    }

    public int getIngestedCount() {
        return ingestedCount;
    }

    public String getRequestId() {
        return requestId;
    }
}
