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
        assertTrue(content.contains("PUBLISH_AEGIS_AEGIS_STATE_EVENT_STREAM"),
                "should contain package-prefixed publish alias");
        assertTrue(content.contains("SUBSCRIBE_AEGIS_AEGIS_STATE_EVENT_STREAM"),
                "should contain package-prefixed subscribe alias");
        assertTrue(content.contains("/SubscribeAegisAegisStateEventStream"),
                "package-prefixed subscribe alias should match generated subscriber naming");

        // Service name
        assertTrue(content.contains("eventbus.EventBusTypedService"),
                "should reference typed service name");
        assertTrue(content.contains("import com.google.protobuf.DynamicMessage;"),
                "subscribe responses should use the metadata envelope type");
        assertTrue(content.contains("MethodDescriptor<Eventbus.SubscribeRequest, DynamicMessage>"),
                "subscribe method descriptors should marshal metadata-bearing envelopes");
        assertTrue(content.contains("ProtoUtils.marshaller(SubscribeHandler.subscribeRecordDefaultInstance())"),
                "subscribe method descriptors should use the EventBus envelope marshaller");
        assertTrue(content.contains("StreamObserver<DynamicMessage> resp"),
                "subscribe handlers should receive envelope response observers");
        assertFalse(content.contains("MethodDescriptor<Eventbus.SubscribeRequest, Aegis.TopologyEvent>"),
                "subscribe method descriptors must not stream bare payloads");

        // bindAll method
        assertTrue(content.contains("bindAll"), "should contain bindAll method");

        // Uses PublishHandler and SubscribeHandler
        assertTrue(content.contains("PublishHandler"), "should reference PublishHandler");
        assertTrue(content.contains("SubscribeHandler"), "should reference SubscribeHandler");

        // Uses TopicRegistry constants
        assertTrue(content.contains("TopicRegistry.TOPIC_AEGIS_STATE_EVENT_STREAM"),
                "should reference TopicRegistry constants");
    }

    /**
     * The defect this pins: tacplus's generated PROTO declares
     * <pkg>.<Topic>StateService/PublishTo<Field>, the broker bound only the flat
     * eventbus.EventBusTypedService/Publish<Vservice><Topic>, and nothing
     * asserted the two agreed. Generated clients calling the per-topic path got
     * "Unimplemented: Method not found" while the fleet reported healthy.
     */
    @Test
    void generate_bindsPerTopicServiceDeclaredByTheProto() throws IOException {
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream",
                "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream",
                "services/aegis");

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(List.of(topic));

        String content = Files.readString(tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java"));

        assertTrue(content.contains("\"aegis.AegisStateStateService/PublishToEventStream\""),
                "must bind the per-topic publish path the generated proto declares");
        assertTrue(content.contains("\"aegis.AegisStateStateService/SubscribeToEventStream\""),
                "must bind the per-topic subscribe path the generated proto declares");
        assertTrue(content.contains("ServerServiceDefinition.builder(\"aegis.AegisStateStateService\")"),
                "per-topic methods need their OWN ServerServiceDefinition — io.grpc "
                        + "requires a method's service prefix to match the definition holding it");
        assertTrue(content.contains("buildPerTopicLazy"),
                "the per-topic definitions must be reachable from the handler");

        // Additive, not a swap. Every publisher that has not yet regenerated is
        // still calling the flat name; removing it here breaks them at runtime.
        assertTrue(content.contains("eventbus.EventBusTypedService"),
                "the flat service must remain bound until every publisher has regenerated");
        assertTrue(content.contains("PUBLISH_AEGIS_STATE_EVENT_STREAM"),
                "the flat descriptors must remain");
    }

    /**
     * The trace topic is the case where the two packages diverge: the state
     * message TraceEventBus is declared in package agentsensor, while its event
     * type is trace.TraceEvent. Deriving the service name from the event type
     * produced "trace.TraceEventBusStateService" — a name no proto declares —
     * and it went unnoticed because the OTHER topic's event type happens to sit
     * in the same package as its state message, so the wrong derivation gave the
     * right answer there.
     */
    @Test
    void generate_namesPerTopicServiceByTheDeclaringPackageNotTheEventType() throws IOException {
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "TraceEventBus", "trace", "trace_events",
                "trace.TraceEvent",
                "vservices.agent-sensor.trace.TraceEventBus.TraceEvents",
                "vservices/agent-sensor", "vservices", false,
                "agentsensor");

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(List.of(topic));

        String content = Files.readString(tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java"));

        assertTrue(content.contains("\"agentsensor.TraceEventBusStateService/PublishToTraceEvents\""),
                "per-topic service must be named by the DECLARING package (agentsensor)");
        assertFalse(content.contains("\"trace.TraceEventBusStateService"),
                "must not name the service by the event type's package — no proto declares that");

        // The Kafka topic name is built from the event type's package and must
        // NOT move: renaming a live topic orphans its retained events.
        assertTrue(content.contains("vservices.agent-sensor.trace.TraceEventBus.TraceEvents")
                        || content.contains("TOPIC_TRACE_EVENT_BUS_TRACE_EVENTS"),
                "the Kafka topic binding must be untouched by the service-name fix");
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
        assertTrue(content.contains("PUBLISH_AEGIS_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("PUBLISH_AEGIS_STATE_CONVERSATIONS"));
        assertTrue(content.contains("PUBLISH_AEGIS_AEGIS_STATE_CONVERSATIONS"));
        assertTrue(content.contains("SUBSCRIBE_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("SUBSCRIBE_AEGIS_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("SUBSCRIBE_AEGIS_STATE_CONVERSATIONS"));
        assertTrue(content.contains("SUBSCRIBE_AEGIS_AEGIS_STATE_CONVERSATIONS"));
    }

    @Test
    void generate_matchesTacplusPackagePrefixedSubscriberNames() throws IOException {
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "AgenticCodeEventBus", "claudesensor", "event_stream",
                "claudesensor.SensorEvent",
                "vservices.claude-sensor.claudesensor.AgenticCodeEventBus.EventStream",
                "vservices/claude-sensor",
                "vservices",
                true);

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services.model");
        gen.generate(List.of(topic));

        Path outputFile = tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java");
        String content = Files.readString(outputFile);

        assertTrue(content.contains("/PublishAgenticCodeEventBusEventStream"),
                "legacy publisher method should remain registered");
        assertTrue(content.contains("/PublishClaudesensorAgenticCodeEventBusEventStream"),
                "canonical publisher method should include proto package prefix");
        assertTrue(content.contains("/SubscribeAgenticCodeEventBusEventStream"),
                "legacy subscriber method should remain registered");
        assertTrue(content.contains("/SubscribeClaudesensorAgenticCodeEventBusEventStream"),
                "canonical subscriber method should include proto package prefix");
    }
}
