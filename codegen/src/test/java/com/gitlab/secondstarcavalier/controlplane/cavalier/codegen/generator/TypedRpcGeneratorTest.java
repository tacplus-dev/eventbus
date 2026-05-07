// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.codegen.generator;

import com.gitlab.secondstarcavalier.controlplane.cavalier.codegen.TopicDiscovery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TypedRpcGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void generate_createsBindingsFile() throws IOException {
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream",
                "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream",
                "services/aegis");

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(List.of(topic));

        Path outputFile = tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java");
        assertTrue(Files.exists(outputFile), "TypedEventBusBindings.java should be generated");

        String content = Files.readString(outputFile);

        // Method descriptors
        assertTrue(content.contains("PUBLISH_AEGIS_STATE_EVENT_STREAM"),
                "should contain publish method descriptor");
        assertTrue(content.contains("SUBSCRIBE_AEGIS_STATE_EVENT_STREAM"),
                "should contain subscribe method descriptor");

        // Service name
        assertTrue(content.contains("eventbus.EventBusTypedService"),
                "should reference typed service name");

        // bindAll method
        assertTrue(content.contains("bindAll"), "should contain bindAll method");

        // Uses PublishHandler and SubscribeHandler
        assertTrue(content.contains("PublishHandler"), "should reference PublishHandler");
        assertTrue(content.contains("SubscribeHandler"), "should reference SubscribeHandler");

        // Uses TopicRegistry constants
        assertTrue(content.contains("TopicRegistry.TOPIC_AEGIS_STATE_EVENT_STREAM"),
                "should reference TopicRegistry constants");
    }

    @Test
    void generate_multipleTopics() throws IOException {
        List<TopicDiscovery.TopicField> topics = List.of(
                new TopicDiscovery.TopicField(
                        "AegisState", "aegis", "event_stream",
                        "aegis.TopologyEvent",
                        "services.aegis.aegis.AegisState.EventStream",
                        "services/aegis"),
                new TopicDiscovery.TopicField(
                        "AegisState", "aegis", "conversations",
                        "aegis.ConversationEvent",
                        "services.aegis.aegis.AegisState.Conversations",
                        "services/aegis")
        );

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(topics);

        Path outputFile = tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java");
        String content = Files.readString(outputFile);

        assertTrue(content.contains("PUBLISH_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("PUBLISH_AEGIS_STATE_CONVERSATIONS"));
        assertTrue(content.contains("SUBSCRIBE_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("SUBSCRIBE_AEGIS_STATE_CONVERSATIONS"));
    }
}
