// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.tacplus.s2sm.ServiceManager;

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
