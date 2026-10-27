// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.codegen.generator;

import com.github.tacplusdev.eventbus.codegen.TopicDiscovery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TypedRpcGeneratorTest {

    @TempDir
    Path tempDir;

    /**
     * Attaches the per-topic auth_services a real run reads out of the registry.
     * The generator refuses to emit a topic that declares none, so a hand-built
     * TopicField has to carry one.
     */
    private static List<TopicDiscovery.TopicField> withAuth(List<TopicDiscovery.TopicField> topics) {
        for (TopicDiscovery.TopicField t : topics) {
            Map<String, List<String>> auth = new LinkedHashMap<>();
            auth.put(t.getPublishRpcName(), List.of("someservice"));
            auth.put(t.getSubscribeRpcName(), List.of("someservice"));
            t.setMethodAuthServices(auth);
        }
        return topics;
    }

    @Test
    void generate_createsBindingsFile() throws IOException {
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream",
                "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream",
                "services/aegis");

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(withAuth(List.of(topic)));

        Path outputFile = tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java");
        assertTrue(Files.exists(outputFile), "TypedEventBusBindings.java should be generated");

        String content = Files.readString(outputFile);

        // Method descriptors
        assertTrue(content.contains("PUBLISH_TO_AEGIS_STATE_EVENT_STREAM"),
                "should contain publish method descriptor");
        assertTrue(content.contains("SUBSCRIBE_TO_AEGIS_STATE_EVENT_STREAM"),
                "should contain subscribe method descriptor");
        assertTrue(content.contains("PUBLISH_TO_AEGIS_STATE_EVENT_STREAM"),
                "should contain package-prefixed publish alias");
        assertTrue(content.contains("SUBSCRIBE_TO_AEGIS_STATE_EVENT_STREAM"),
                "should contain package-prefixed subscribe alias");
        // The wire method is now the per-topic one. The package-prefixed flat
        // alias it used to assert was one of two flat spellings the broker
        // carried; both are gone.
        assertTrue(content.contains("/SubscribeToEventStream"),
                "subscribe method must match the per-topic name the proto declares");

        // Service name
        assertFalse(content.contains("eventbus.EventBusTypedService"),
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
        // bindAll is gone with the flat service it built. Per-topic definitions
        // are returned by buildPerTopicLazy, one per topic, because io.grpc ties
        // a method's service prefix to the definition holding it.
        assertTrue(content.contains("buildPerTopicLazy"), "should expose the per-topic builder");
        assertFalse(content.contains("ServerServiceDefinition bindAll"), "flat bindAll must be gone");

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
        gen.generate(withAuth(List.of(topic)));

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
        assertFalse(content.contains("eventbus.EventBusTypedService"),
                "the flat service must remain bound until every publisher has regenerated");
        assertTrue(content.contains("PUBLISH_TO_AEGIS_STATE_EVENT_STREAM"),
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
        gen.generate(withAuth(List.of(topic)));

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
        gen.generate(withAuth(topics));

        Path outputFile = tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java");
        String content = Files.readString(outputFile);

        assertTrue(content.contains("PUBLISH_TO_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("PUBLISH_TO_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("PUBLISH_TO_AEGIS_STATE_CONVERSATIONS"));
        assertTrue(content.contains("PUBLISH_TO_AEGIS_STATE_CONVERSATIONS"));
        assertTrue(content.contains("SUBSCRIBE_TO_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("SUBSCRIBE_TO_AEGIS_STATE_EVENT_STREAM"));
        assertTrue(content.contains("SUBSCRIBE_TO_AEGIS_STATE_CONVERSATIONS"));
        assertTrue(content.contains("SUBSCRIBE_TO_AEGIS_STATE_CONVERSATIONS"));
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
        gen.generate(withAuth(List.of(topic)));

        Path outputFile = tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java");
        String content = Files.readString(outputFile);

        assertTrue(content.contains("/PublishToEventStream"),
                "per-topic publisher method must be registered");
        assertTrue(content.contains("claudesensor.AgenticCodeEventBusStateService"),
                "canonical publisher method should include proto package prefix");
        assertTrue(content.contains("/SubscribeToEventStream"),
                "per-topic subscriber method must be registered");
        // Both flat spellings — bare and package-prefixed — are gone. The
        // package now qualifies the SERVICE, not the method name.
        assertFalse(content.contains("SubscribeAgenticCodeEventBusEventStream"),
                "flat subscriber method must be gone");
        assertFalse(content.contains("SubscribeClaudesensorAgenticCodeEventBusEventStream"),
                "package-prefixed flat subscriber method must be gone");
    }

    @Test
    void generate_emitsPerTopicAuthServices() throws IOException {
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream",
                "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream",
                "services/aegis");
        Map<String, List<String>> auth = new LinkedHashMap<>();
        auth.put("PublishToEventStream", List.of("aegis"));
        auth.put("SubscribeToEventStream", List.of("aegis", "sentinel"));
        topic.setMethodAuthServices(auth);

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(List.of(topic));

        String content = Files.readString(tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java"));
        assertTrue(content.contains(
                        "authorizer.setMethodAuth(\"aegis.AegisStateStateService/PublishToEventStream\", "
                        + "List.of(\"aegis\"));"),
                "publish policy should come straight from the registry, keyed by full path: " + content);
        assertTrue(content.contains(
                        "authorizer.setMethodAuth(\"aegis.AegisStateStateService/SubscribeToEventStream\", "
                        + "List.of(\"aegis\", \"sentinel\"));"),
                "subscribe policy should carry every declared caller: " + content);
    }

    @Test
    void generate_refusesATopicWithNoAuthServices() {
        // Fail-closed at generation: a topic with no policy would otherwise be
        // served with none, which is exactly the F15 hole.
        TopicDiscovery.TopicField topic = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream",
                "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream",
                "services/aegis");

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> gen.generate(List.of(topic)));
        assertTrue(e.getMessage().contains("aegis.AegisStateStateService"), e.getMessage());
    }

    @Test
    void generate_keepsCollidingRpcNamesApartByFullPath() throws IOException {
        // Topics genuinely share rpc names — every topic field named "stream"
        // spells PublishToStream — and they carry DIFFERENT auth_services. Keyed
        // by bare rpc name one policy would silently overwrite the other, handing
        // one topic's callers access to the other's. Keyed by full grpc path they
        // cannot collide.
        TopicDiscovery.TopicField a = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "stream", "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.Stream", "services/aegis");
        Map<String, List<String>> authA = new LinkedHashMap<>();
        authA.put("PublishToStream", List.of("aegis"));
        authA.put("SubscribeToStream", List.of("aegis"));
        a.setMethodAuthServices(authA);

        TopicDiscovery.TopicField b = new TopicDiscovery.TopicField(
                "OtherState", "other", "stream", "other.OtherEvent",
                "services.other.other.OtherState.Stream", "services/other");
        Map<String, List<String>> authB = new LinkedHashMap<>();
        authB.put("PublishToStream", List.of("other"));
        authB.put("SubscribeToStream", List.of("other"));
        b.setMethodAuthServices(authB);

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(List.of(a, b));

        String content = Files.readString(tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java"));
        assertTrue(content.contains(
                        "authorizer.setMethodAuth(\"aegis.AegisStateStateService/PublishToStream\", "
                        + "List.of(\"aegis\"));"),
                "aegis keeps its own policy: " + content);
        assertTrue(content.contains(
                        "authorizer.setMethodAuth(\"other.OtherStateStateService/PublishToStream\", "
                        + "List.of(\"other\"));"),
                "other keeps its own policy: " + content);
    }

    /**
     * (spec.store_namespace): a topic whose type declares the namespace field
     * must be wired with a namespace stamper; one that does not must be wired
     * with null. The null is load-bearing — PublishHandler refuses a publish
     * for want of the caller's namespace claim only when the stamper is
     * present, so emitting one for a type with no such field would break every
     * publish to it.
     */
    @Test
    void generate_wiresNamespaceStamperOnlyForDeclaringTypes() throws IOException {
        TopicDiscovery.TopicField withNs = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "conversations", "aegis.ConversationEvent",
                "services.aegis.aegis.AegisState.Conversations", "services/aegis",
                "services", true, "aegis", false, true);
        TopicDiscovery.TopicField withoutNs = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream", "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream", "services/aegis");

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(withAuth(List.of(withNs, withoutNs)));

        String content = Files.readString(tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java"));

        assertTrue(content.contains(
                        "(msg, namespace) -> msg.toBuilder().setNamespace(namespace).build()"),
                "a type declaring namespace must be wired with a stamper: " + content);
        assertEquals(1, content.split("setNamespace\\(namespace\\)", -1).length - 1,
                "exactly one topic declares namespace, so exactly one stamper: " + content);
    }

    /**
     * The read half, on the SUBSCRIBE binding (item 4). SubscribeHandler pins
     * delivery to the caller's own tenant only when it has a namespace reader,
     * so the null for a non-declaring type is as load-bearing as the stamper's:
     * emitting a reader for a type with no such field would not compile, and
     * omitting one for a type that has it leaves the topic fleet-wide.
     */
    @Test
    void generate_wiresSubscribeNamespaceReaderOnlyForDeclaringTypes() throws IOException {
        TopicDiscovery.TopicField withNs = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "conversations", "aegis.ConversationEvent",
                "services.aegis.aegis.AegisState.Conversations", "services/aegis",
                "services", true, "aegis", false, true);
        TopicDiscovery.TopicField withoutNs = new TopicDiscovery.TopicField(
                "AegisState", "aegis", "event_stream", "aegis.TopologyEvent",
                "services.aegis.aegis.AegisState.EventStream", "services/aegis");

        TypedRpcGenerator gen = new TypedRpcGenerator(tempDir, "com.example.services");
        gen.generate(withAuth(List.of(withNs, withoutNs)));

        String content = Files.readString(tempDir.resolve(
                "src/main/java/com/example/services/generated/TypedEventBusBindings.java"));

        assertTrue(content.contains("::parseFrom, Aegis.ConversationEvent::getNamespace)"),
                "the declaring type's subscribe binding must carry a namespace reader: " + content);
        assertTrue(content.contains("::parseFrom, null)"),
                "the non-declaring type's subscribe binding must pass null: " + content);
        assertEquals(1, content.split("::parseFrom, null\\)", -1).length - 1,
                "exactly one topic declares no namespace, so exactly one null reader: " + content);
    }
}
