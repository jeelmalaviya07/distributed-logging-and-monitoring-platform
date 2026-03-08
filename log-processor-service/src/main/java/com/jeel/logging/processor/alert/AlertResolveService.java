package com.jeel.logging.processor.alert;
import com.jeel.logging.processor.persistence.repository.AlertHistoryRepository;
import com.jeel.logging.processor.persistence.repository.AlertStateRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class AlertResolveService {

    private final AlertStateRepository stateRepository;
    private final AlertHistoryRepository historyRepository;

    public AlertResolveService(
            AlertStateRepository stateRepository,
            AlertHistoryRepository historyRepository
    ) {
        this.stateRepository = stateRepository;
        this.historyRepository = historyRepository;
    }

    public void resolve(String tenantId, String serviceName, String groupId) {

        AlertState state =
                stateRepository.findByTenantIdAndServiceNameAndGroupId(
                        tenantId,
                        serviceName,
                        groupId
                ).orElseThrow(() -> new RuntimeException("Alert not found"));
        state.setState("OK");
        state.setAcknowledged(true);
        state.setUpdatedAt(Instant.now());

        stateRepository.save(state);

        AlertHistory history = new AlertHistory();

        history.setRuleId(0L);
        history.setTenantId(tenantId);
        history.setServiceName(serviceName);
        history.setSeverity("RESOLVED");
        history.setTriggeredCount(0L);
        history.setTriggeredAt(Instant.now());

        historyRepository.save(history);
    }
}