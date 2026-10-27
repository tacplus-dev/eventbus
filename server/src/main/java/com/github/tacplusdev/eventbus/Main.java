// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus;

import com.github.tacplusdev.eventbus.model.s2sm.ServiceManager;

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
