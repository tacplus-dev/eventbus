// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus;

import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.ServiceManager;

/**
 * Entry point for the event-bus service.
 * Bridges gRPC streaming RPCs to Kafka topics with replay support.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        ServiceManager sm = ServiceManager.create(args);
        sm.run(new EventBusHandler(sm));
    }
}
