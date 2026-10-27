// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * logback.xml pins the application's own loggers to INFO by package name, so
 * that name has to follow the package the service's classes live in: a stale
 * prefix configures nothing and leaves them to whatever the root level is.
 */
class LogbackConfigTest {

    @Test
    void applicationLogger_coversTheServicePackage() throws Exception {
        String servicePackage = Main.class.getPackageName();
        try (InputStream in = Main.class.getResourceAsStream("/logback.xml")) {
            assertNotNull(in, "logback.xml on the classpath");
            NodeList loggers = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                    .parse(in).getElementsByTagName("logger");
            for (int i = 0; i < loggers.getLength(); i++) {
                Element logger = (Element) loggers.item(i);
                String name = logger.getAttribute("name");
                if (servicePackage.equals(name) || servicePackage.startsWith(name + ".")) {
                    assertEquals("INFO", logger.getAttribute("level"));
                    return;
                }
            }
            fail("no <logger> in logback.xml covers " + servicePackage);
        }
    }
}
