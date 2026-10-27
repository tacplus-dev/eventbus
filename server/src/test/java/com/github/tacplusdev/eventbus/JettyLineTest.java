// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus;

import org.eclipse.jetty.util.Jetty;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Jetty reaches the server through Javalin (eventbus-model). Jetty 11 has HIGH
 * advisories (request smuggling, Digest-auth bypass) whose fixes are not on
 * Maven Central, so the only way off them is Jetty 12 via Javalin 7 (#9): a
 * Javalin 6 coming back, or anything else pulling Jetty 11 in, fails here.
 */
class JettyLineTest {

    @Test
    void resolvedJetty_isNotTheUnpatchedJetty11Line() {
        String version = Jetty.VERSION;
        int major = Integer.parseInt(version.substring(0, version.indexOf('.')));
        assertTrue(major >= 12, "Jetty " + version + " on the classpath; Jetty 11 is unpatched");
    }
}
