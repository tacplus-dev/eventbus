// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.codegen;

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

    @Test
    void discoverTopics_javaTypeSuffixedWhenServiceSharesOuterClassName() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry-outerclass.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        // package "tasks" also declares "service Tasks", so protoc — and tacplus's
        // java_outer_classname — call the outer class TasksOuterClass. Importing
        // plain "Tasks" does not compile.
        TopicDiscovery.TopicField taskEvents = topics.stream()
                .filter(t -> t.getFieldName().equals("task_events"))
                .findFirst()
                .orElseThrow();
        assertEquals("com.example.services.types.services.tasks.TasksOuterClass",
                taskEvents.getJavaOuterClassImport("com.example.services"));
        assertEquals("TasksOuterClass.TaskEvent", taskEvents.getJavaQualifiedType());

        // package "aegis" has no such collision and keeps the plain name.
        TopicDiscovery.TopicField eventStream = topics.stream()
                .filter(t -> t.getFieldName().equals("event_stream"))
                .findFirst()
                .orElseThrow();
        assertEquals("com.example.services.types.services.aegis.Aegis",
                eventStream.getJavaOuterClassImport("com.example.services"));
        assertEquals("Aegis.TopologyEvent", eventStream.getJavaQualifiedType());
    }

    /**
     * Item 6 Piece A: a topic type declaring only spiffe_id (not
     * actor_spiffe_id) must report hasSpiffeIdField() true and
     * hasActorSpiffeIdField() false — the two are independent flags.
     */
    @Test
    void discoverTopics_hasSpiffeIdFieldWithoutActor() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        TopicDiscovery.TopicField eventStream = topics.stream()
                .filter(t -> t.getFieldName().equals("event_stream"))
                .findFirst()
                .orElseThrow();

        assertTrue(eventStream.hasSpiffeIdField(), "aegis.TopologyEvent declares spiffe_id");
        assertFalse(eventStream.hasActorSpiffeIdField(),
                "aegis.TopologyEvent declares no actor_spiffe_id");
    }

    /**
     * A topic type declaring BOTH spiffe_id and actor_spiffe_id (the item 6
     * Piece A opt-in shape) must report both flags true.
     */
    @Test
    void discoverTopics_hasActorSpiffeIdFieldWhenDeclared() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        TopicDiscovery.TopicField conversations = topics.stream()
                .filter(t -> t.getFieldName().equals("conversations"))
                .findFirst()
                .orElseThrow();

        assertTrue(conversations.hasSpiffeIdField(), "aegis.ConversationEvent declares spiffe_id");
        assertTrue(conversations.hasActorSpiffeIdField(),
                "aegis.ConversationEvent declares actor_spiffe_id");
    }

    /**
     * (spec.store_namespace): a topic type declaring a string namespace field
     * must report hasNamespaceField() true, and one declaring none must report
     * false. The flag is what decides whether a publish to the topic is refused
     * for want of the caller's namespace claim, so a type that does not declare
     * the field must never acquire it by accident.
     */
    @Test
    void discoverTopics_hasNamespaceFieldOnlyWhenDeclared() throws IOException {
        Path registryPath = Path.of(getClass().getClassLoader()
                .getResource("test-registry.json").getPath());

        TopicDiscovery discovery = new TopicDiscovery(registryPath);
        List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

        TopicDiscovery.TopicField conversations = topics.stream()
                .filter(t -> t.getFieldName().equals("conversations"))
                .findFirst()
                .orElseThrow();
        assertTrue(conversations.hasNamespaceField(),
                "aegis.ConversationEvent declares namespace");

        TopicDiscovery.TopicField eventStream = topics.stream()
                .filter(t -> t.getFieldName().equals("event_stream"))
                .findFirst()
                .orElseThrow();
        assertFalse(eventStream.hasNamespaceField(),
                "aegis.TopologyEvent declares no namespace");
    }
}

