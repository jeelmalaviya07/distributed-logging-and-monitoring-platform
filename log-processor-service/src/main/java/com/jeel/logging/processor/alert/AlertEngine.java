package com.jeel.logging.processor.alert;

import com.jeel.logging.processor.kafka.AlertNotificationPublisher;
import com.jeel.logging.processor.persistence.repository.AlertHistoryRepository;
import com.jeel.logging.processor.persistence.repository.AlertStateRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Service
public class AlertEngine {

    private final AlertStateRepository stateRepository;
    private final AlertHistoryRepository historyRepository;
    private final AlertNotificationPublisher publisher;

    public AlertEngine(
            AlertStateRepository stateRepository,
            AlertHistoryRepository historyRepository,
            AlertNotificationPublisher publisher
    ) {
        this.stateRepository = stateRepository;
        this.historyRepository = historyRepository;
        this.publisher = publisher;
    }

    public void evaluate(
            String tenantId,
            String serviceName,
            String groupId,
            long count
    ) {

        Optional<AlertState> existing =
                stateRepository.findByTenantIdAndServiceNameAndGroupId(
                        tenantId, serviceName, groupId
                );

        AlertState state;

        if (existing.isEmpty()) {

            state = new AlertState();

            state.setTenantId(tenantId);
            state.setServiceName(serviceName);
            state.setGroupId(groupId);

            state.setState("FIRING");
            state.setAcknowledged(false);
            state.setTriggeredCount(count);

            state.setCreatedAt(Instant.now());
            state.setUpdatedAt(Instant.now());
            state.setLastTriggeredAt(Instant.now());

            stateRepository.save(state);

            recordHistory(state);
            publish(state);

            return;
        }

        state = existing.get();

        if ("FIRING".equals(state.getState())) {

            state.setTriggeredCount(count);
            state.setUpdatedAt(Instant.now());

            stateRepository.save(state);
            return;
        }

        state.setState("FIRING");
        state.setTriggeredCount(count);
        state.setAcknowledged(false);
        state.setUpdatedAt(Instant.now());
        state.setLastTriggeredAt(Instant.now());

        stateRepository.save(state);

        recordHistory(state);
        publish(state);
    }

    private void recordHistory(AlertState state) {

        AlertHistory history = new AlertHistory();

        history.setRuleId(0L);
        history.setTenantId(state.getTenantId());
        history.setServiceName(state.getServiceName());
        history.setSeverity("ERROR");
        history.setTriggeredCount(state.getTriggeredCount());
        history.setTriggeredAt(Instant.now());

        historyRepository.save(history);
    }

    private void publish(AlertState state) {

        AlertNotificationEvent event = new AlertNotificationEvent();

        event.setTenantId(state.getTenantId());
        event.setServiceName(state.getServiceName());
        event.setGroupId(state.getGroupId());
        event.setState(state.getState());
        event.setTriggeredCount(state.getTriggeredCount());
        event.setTimestamp(Instant.now());

        publisher.publish(event);
    }
}