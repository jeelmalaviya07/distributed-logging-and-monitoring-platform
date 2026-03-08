package com.jeel.logging.processor.api;

import com.jeel.logging.processor.alert.AlertResolveService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/alerts")
public class AlertCommandController {

    private final AlertResolveService resolveService;

    public AlertCommandController(AlertResolveService resolveService) {
        this.resolveService = resolveService;
    }

    @PatchMapping("/resolve")
    public void resolveAlert(
            @RequestParam String tenantId,
            @RequestParam String serviceName,
            @RequestParam String groupId
    ) {

        resolveService.resolve(
                tenantId,
                serviceName,
                groupId
        );
    }
}