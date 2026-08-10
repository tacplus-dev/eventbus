// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.codegen.generator;

import com.gitlab.secondstarcavalier.controlplane.schematic.codegen.TopicDiscovery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopicRegistryGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void generate_createsTopicRegistryFile() throws IOException {
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream",
                "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream",
                "services/aegis");

        TopicRegistryGenerator gen = new TopicRegistryGenerator(tempDir, "com.example.services");
        gen.generate(List.of(topic));

        Path outputFile = tempDir.resolve("src/main/java/com/example/services/generated/TopicRegistry.java");
        assertTrue(Files.exists(outputFile), "TopicRegistry.java should be generated");

        String content = Files.readString(outputFile);
        assertTrue(content.contains("TOPIC_AEGIS_STATE_EVENT_STREAM"),
                "should contain topic constant");
        assertTrue(content.contains("services.aegis.aegis.AegisState.EventStream"),
                "should contain topic name value");
        assertTrue(content.contains("DO NOT EDIT"), "should have codegen header");
    }
}
