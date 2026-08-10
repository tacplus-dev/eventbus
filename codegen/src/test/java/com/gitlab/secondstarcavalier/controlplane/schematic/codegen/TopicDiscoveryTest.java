// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.codegen;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopicDiscoveryTest {

    @Test
    void discoverTopics_findsOnlyEventbusTopics() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        // Should find 2 eventbus topics (event_stream and conversations),
        // not the singleton (topology_graph has graph_type "singleton", not "topic")
        // and not the kernel boundary.
        assertEquals(2, topics.size());
    }

    @Test
    void discoverTopics_correctFieldNames() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        TopicDiscovery.TopicField eventStream = topics.stream()
                .filter(t -> t.getFieldName().equals("event_stream"))
                .findFirst()
                .orElseThrow();

        assertEquals("AegisState", eventStream.getStateName());
        assertEquals("aegis", eventStream.getStatePackage());
        assertEquals("aegis.TopologyEvent", eventStream.getFieldType());
        assertEquals("EventStream", eventStream.getFieldPascal());
        assertEquals("services/aegis", eventStream.getPackagePath());
    }

    @Test
    void discoverTopics_topicNameConvention() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        TopicDiscovery.TopicField eventStream = topics.stream()
                .filter(t -> t.getFieldName().equals("event_stream"))
                .findFirst()
                .orElseThrow();

        // Topic name: {packagePath.dots}.{protoPackage}.{StateRoot}.{FieldPascal}
        assertEquals("services.aegis.aegis.AegisState.EventStream", eventStream.getTopicName());
    }

    @Test
    void discoverTopics_javaType() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        TopicDiscovery.TopicField eventStream = topics.stream()
                .filter(t -> t.getFieldName().equals("event_stream"))
                .findFirst()
                .orElseThrow();

        String outerImport = eventStream.getJavaOuterClassImport("com.example.services");
        assertEquals("com.example.services.types.services.aegis.Aegis", outerImport);

        String qualifiedType = eventStream.getJavaQualifiedType();
        assertEquals("Aegis.TopologyEvent", qualifiedType);
    }

    @Test
    void discoverTopics_excludesNonTopicBoundaries() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        // topology_graph has graph_type "singleton", should not be discovered
        boolean hasSingleton = topics.stream()
                .anyMatch(t -> t.getFieldName().equals("topology_graph"));
        assertFalse(hasSingleton, "singleton boundaries should not be discovered as topics");

        // kernel boundary should not be discovered (tag != "eventbus")
        boolean hasKernel = topics.stream()
                .anyMatch(t -> t.getStateName().equals("Cluster"));
        assertFalse(hasKernel, "kernel-store boundaries should not be discovered");
    }
}
