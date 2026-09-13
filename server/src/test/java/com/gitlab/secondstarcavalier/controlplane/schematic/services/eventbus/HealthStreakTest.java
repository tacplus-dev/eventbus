// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.types.sys.healthreport.Healthreport.InstanceStatus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * checkHealth() must tolerate transient Kafka probe failures — a rebalance or a
 * recovering broker is not a wedged controller — and only report UNHEALTHY after
 * three consecutive failures.
 */
class HealthStreakTest {

    private static EventBusHandler handler() {
        return new EventBusHandler(null);
    }

    @Test
    void singleFailureStaysHealthy() {
        EventBusHandler h = handler();
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
    }

    @Test
    void twoFailuresThenSuccessNeverReportsUnhealthy() {
        EventBusHandler h = handler();
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(true, null));
    }

    @Test
    void threeConsecutiveFailuresReportUnhealthy() {
        EventBusHandler h = handler();
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
        assertEquals(InstanceStatus.INSTANCE_STATUS_UNHEALTHY, h.recordProbe(false, "timeout"));
        // Stays unhealthy while the failures continue.
        assertEquals(InstanceStatus.INSTANCE_STATUS_UNHEALTHY, h.recordProbe(false, "timeout"));
    }

    @Test
    void successResetsTheStreak() {
        EventBusHandler h = handler();
        h.recordProbe(false, "timeout");
        h.recordProbe(false, "timeout");
        h.recordProbe(true, null);
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
        assertEquals(InstanceStatus.INSTANCE_STATUS_UNHEALTHY, h.recordProbe(false, "timeout"));
    }

    @Test
    void uninitializedHandlerReportsHealthy() {
        assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, handler().checkHealth());
    }
}
