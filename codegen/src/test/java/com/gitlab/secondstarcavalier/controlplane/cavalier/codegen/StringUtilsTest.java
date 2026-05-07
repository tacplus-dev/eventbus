// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.codegen;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StringUtilsTest {

    @Test
    void toPascalCase_basic() {
        assertEquals("EventStream", StringUtils.toPascalCase("event_stream"));
    }

    @Test
    void toPascalCase_singleWord() {
        assertEquals("Events", StringUtils.toPascalCase("events"));
    }

    @Test
    void toPascalCase_multipleUnderscores() {
        assertEquals("MyLongFieldName", StringUtils.toPascalCase("my_long_field_name"));
    }

    @Test
    void toPascalCase_empty() {
        assertEquals("", StringUtils.toPascalCase(""));
    }

    @Test
    void toPascalCase_null() {
        assertNull(StringUtils.toPascalCase(null));
    }

    @Test
    void toLowerCamelCase_basic() {
        assertEquals("eventStream", StringUtils.toLowerCamelCase("event_stream"));
    }

    @Test
    void extractSimpleTypeName_qualified() {
        assertEquals("TopologyEvent", StringUtils.extractSimpleTypeName("aegis.TopologyEvent"));
    }

    @Test
    void extractSimpleTypeName_unqualified() {
        assertEquals("TopologyEvent", StringUtils.extractSimpleTypeName("TopologyEvent"));
    }

    @Test
    void extractSimpleTypeName_deeplyQualified() {
        assertEquals("Foo", StringUtils.extractSimpleTypeName("a.b.c.Foo"));
    }

    @Test
    void extractSimpleTypeName_null() {
        assertNull(StringUtils.extractSimpleTypeName(null));
    }
}
