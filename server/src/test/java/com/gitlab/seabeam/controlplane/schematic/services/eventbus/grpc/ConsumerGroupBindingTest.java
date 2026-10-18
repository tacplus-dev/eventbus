// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus.grpc;

import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.KafkaEmbeddedBroker;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.KafkaSubscriber;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.TopicManager;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.key.ImmutableKey;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.key.KeyHelpers;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Auth;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.token.TokenWrapper;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.key.KeyOuterClass;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BIG GOAL 7 item 6: the client-supplied consumer_group reached Kafka verbatim.
 *
 * <p>SubscribeHandler passed {@code SubscribeRequest.consumer_group} straight
 * into {@code KafkaSubscriber.createSession}, which used it as the Kafka
 * {@code group.id}. Nothing bound it to the caller, so any authenticated caller
 * could name another service's group (stratum's ingestion group, say) and take
 * over its share of the partitions.
 *
 * <p>These are real sessions against a real embedded Kafka broker, driven
 * through the real SubscribeHandler under a real gRPC context carrying a
 * verified token — the same seam production identity comes from.
 *
 * <p>{@link #negativeControl_rawGroupId_landsOnTheVictimsGroup} is the guard
 * against a vacuous suite: it drives the PRE-FIX path (the raw client value
 * handed to createSession) and asserts the attacker really does land on the
 * victim's group id, so the isolation assertions below mean something.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConsumerGroupBindingTest {

    private static final String TOPIC = "test.groupbinding.Events";

    /** The group id stratum's cold-start rebuild really uses (ColdStartRebuilder). */
    private static final String VICTIM_GROUP = "stratum-rebuild";

    private static final String VICTIM_SUB = "spiffe://test/system/services/stratum";
    private static final String ATTACKER_SUB = "spiffe://test/system/services/tasks";

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;
    private static ImmutableKey signingKey;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("group-binding-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(2, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39293));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39292, 39293,
                voters, clusterId, defaults, null);

        bootstrapAddress = broker.localAddress();
        Thread.sleep(3000);

        topicManager = new TopicManager(bootstrapAddress, 2, 1, 1);
        topicManager.ensureTopicExists(TOPIC);

        KeyOuterClass.Key proto = KeyHelpers.newEd25519Key();
        signingKey = ImmutableKey.from(proto);
    }

    @AfterAll
    static void stopBroker() {
        if (topicManager != null) topicManager.close();
        if (broker != null) broker.close();
    }

    // ---- the pre-fix path, as a negative control ----

    /**
     * What the code did before: the client's string, used verbatim. The
     * attacker's session joins the victim's group id itself.
     */
    @Test
    @Order(1)
    void negativeControl_rawGroupId_landsOnTheVictimsGroup() throws Exception {
        String group = "nc." + VICTIM_GROUP;
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);

        // Attacker names the victim's group; nothing rewrites it.
        KafkaSubscriber.SeekParams params = new KafkaSubscriber.SeekParams(
                KafkaSubscriber.SeekMode.EARLIEST, 0, 0, group,
                false, KafkaSubscriber.BackpressurePolicy.BUFFER, 1, List.of());
        KafkaSubscriber.Session session = subscriber.createSession(TOPIC, params);
        Thread.startVirtualThread(() -> session.run(params, record -> true));

        assertTrue(awaitGroup(group), "pre-fix, the attacker joins the victim's group id verbatim");
    }

    // ---- the fixed path ----

    /**
     * Two identities ask for the SAME group id through the real handler. Neither
     * gets it: each lands in a group namespaced under its own verified sub, and
     * the bare name the attacker aimed at is never created.
     */
    @Test
    @Order(2)
    void twoCallersNamingTheSameGroup_areIsolated() throws Exception {
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);

        subscribeAs(subscriber, VICTIM_SUB, VICTIM_GROUP);
        subscribeAs(subscriber, ATTACKER_SUB, VICTIM_GROUP);

        String victimGroup = VICTIM_SUB + "|" + VICTIM_GROUP;
        String attackerGroup = ATTACKER_SUB + "|" + VICTIM_GROUP;

        assertTrue(awaitGroup(victimGroup), "victim should hold its own namespaced group");
        assertTrue(awaitGroup(attackerGroup), "attacker should be confined to its own namespaced group");
        assertFalse(groupExists(VICTIM_GROUP),
                "the bare group id the attacker asked for must never reach Kafka");
    }

    /**
     * The legitimate case still works: the same identity subscribing twice with
     * its own group id shares one group, which is what offset tracking across
     * replicas depends on.
     */
    @Test
    @Order(3)
    void sameIdentityTwice_sharesOneGroup() throws Exception {
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        String requested = "stratum-shared";

        subscribeAs(subscriber, VICTIM_SUB, requested);
        subscribeAs(subscriber, VICTIM_SUB, requested);

        String bound = VICTIM_SUB + "|" + requested;
        assertTrue(awaitGroup(bound), "both sessions should join one group");
        // The second join rebalances the group; wait for it to settle rather
        // than racing the coordinator.
        assertTrue(awaitCondition(() -> "Stable".equals(topicManager.getConsumerGroupLag(bound).state())),
                "the shared group should settle Stable with both members, was "
                        + topicManager.getConsumerGroupLag(bound).state());

        long matching = topicManager.listConsumerGroups().stream()
                .filter(g -> g.groupId().endsWith("|" + requested))
                .count();
        assertEquals(1, matching, "one identity, one group — not one group per session");
    }

    /**
     * No verified token on the RPC means there is no identity to bind to, so a
     * named group is refused rather than silently accepted unbound.
     */
    @Test
    @Order(4)
    void namedGroupWithoutAVerifiedToken_isRefused() {
        // Bootstrap address is irrelevant: the request never reaches Kafka.
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        AtomicReference<Throwable> error = new AtomicReference<>();
        SubscribeHandler<Empty> handler = new SubscribeHandler<>(
                subscriber, TOPIC, collectingObserver(error), Empty::parseFrom);

        handler.onNext(Eventbus.SubscribeRequest.newBuilder()
                .setConsumerGroup(VICTIM_GROUP)
                .setEarliest(true)
                .build());

        Throwable t = error.get();
        assertNotNull(t, "an unauthenticated named group should fail the stream");
        assertEquals(Status.Code.UNAUTHENTICATED,
                ((StatusRuntimeException) t).getStatus().getCode());
    }

    // ---- helpers ----

    /**
     * Runs one subscribe through the real handler inside a gRPC context whose
     * verified-token slot holds a token for {@code sub} — the same slot the
     * chassis Authorizer fills on a live call.
     */
    private void subscribeAs(KafkaSubscriber subscriber, String sub, String requestedGroup)
            throws Exception {
        TokenWrapper token = TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience("eventbus")
                .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                .setSubType("service")
                .setSubject(sub)
                // Mint-time requirement as of the regenerated TokenWrapper: sign()
                // refuses a token carrying no namespace claim.
                .setNamespace("/local/test");
        token.sign(signingKey);

        AtomicReference<Throwable> error = new AtomicReference<>();
        SubscribeHandler<Empty> handler = new SubscribeHandler<>(
                subscriber, TOPIC, collectingObserver(error), Empty::parseFrom);

        Context ctx = Auth.withVerifiedTokenSlot(Context.current());
        ctx.run(() -> {
            Auth.recordVerifiedToken(token);
            assertEquals(sub, Auth.callerSubFromContext(), "test context should carry the caller sub");
            handler.onNext(Eventbus.SubscribeRequest.newBuilder()
                    .setConsumerGroup(requestedGroup)
                    .setEarliest(true)
                    .build());
        });
        assertTrue(error.get() == null, "subscribe should not have failed: " + error.get());
    }

    private static StreamObserver<DynamicMessage> collectingObserver(AtomicReference<Throwable> error) {
        return new StreamObserver<>() {
            @Override public void onNext(DynamicMessage value) { }
            @Override public void onError(Throwable t) { error.set(t); }
            @Override public void onCompleted() { }
        };
    }

    private static boolean groupExists(String groupId) {
        return topicManager.listConsumerGroups().stream()
                .anyMatch(g -> g.groupId().equals(groupId));
    }

    private static boolean awaitGroup(String groupId) throws Exception {
        return awaitCondition(() -> groupExists(groupId));
    }

    private static boolean awaitCondition(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }
}
