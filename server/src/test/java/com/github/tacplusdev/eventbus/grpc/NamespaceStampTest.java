// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.grpc;

import com.github.tacplusdev.eventbus.kafka.KafkaEmbeddedBroker;
import com.github.tacplusdev.eventbus.kafka.KafkaPublisher;
import com.github.tacplusdev.eventbus.kafka.KafkaSubscriber;
import com.github.tacplusdev.eventbus.kafka.TopicManager;
import com.github.tacplusdev.eventbus.model.chassis.key.ImmutableKey;
import com.github.tacplusdev.eventbus.model.chassis.key.KeyHelpers;
import com.github.tacplusdev.eventbus.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.eventbus.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.eventbus.model.types.services.eventbus.Eventbus;
import com.github.tacplusdev.eventbus.model.types.services.opa.Opa;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * (spec.store_namespace): the row's owning tenant is stamped at publish, from
 * the caller's verified token, or the publish is refused.
 *
 * <p>Every (spec.record) now declares the field, and the generated egress
 * filter holds every c2s read to the reader's own namespace claim. Nothing
 * populated the field, so every row read back "" — a value no legitimate
 * caller's claim can ever equal. This pins the ingest half: the field is
 * stamped from the token on the way in, and a caller whose token carries no
 * namespace claim is refused rather than having its own value pass through
 * (which would make the tenant boundary publisher-controllable).
 *
 * <p>Real embedded Kafka, the real {@link PublishHandler}, and a real
 * Ed25519-signed token in the same verified-token context slot the chassis
 * Authorizer fills on a live call. The stamped record is read back off the
 * broker and parsed, so the assertion is on what actually landed in the log,
 * not on what the handler was asked to do.
 *
 * <p>{@code opa.NamespaceEntitlementsPolicy} stands in for a record type: it is
 * an ordinary generated message carrying a string {@code namespace} field, so
 * its builder has the same {@code setNamespace} the generated stamper lambda
 * calls. This class exercises {@link PublishHandler} directly, with a stamper
 * written out by hand; {@code FleetRecordNamespaceStampTest} exercises the same
 * behaviour on the fleet's own record types through the stamper the generator
 * emitted.
 */
class NamespaceStampTest {

    private static final String CALLER_SUB = "spiffe://test/system/services/agentsensor";
    private static final String CALLER_NAMESPACE = "acme";

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;
    private static KafkaPublisher publisher;
    private static ImmutableKey signingKey;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("namespace-stamp-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39493));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39492, 39493,
                voters, clusterId, defaults, null);

        bootstrapAddress = broker.localAddress();
        Thread.sleep(3000);

        topicManager = new TopicManager(bootstrapAddress, 1, 1, 1);
        publisher = new KafkaPublisher(bootstrapAddress);

        signingKey = ImmutableKey.from(KeyHelpers.newEd25519Key());
    }

    @AfterAll
    static void stopBroker() {
        if (publisher != null) publisher.close();
        if (topicManager != null) topicManager.close();
        if (broker != null) broker.close();
    }

    // ---- the stamp ----

    /**
     * A caller whose verified token carries a namespace claim: the field lands
     * in the Kafka log holding the CLAIM's value, not the publisher's.
     */
    @Test
    void publishWithNamespaceClaim_stampsTheCallersNamespace() throws Exception {
        String topic = "test.namespacestamp.Stamped";
        topicManager.ensureTopicExists(topic);

        // The publisher names someone else's tenant. It must not survive.
        Opa.NamespaceEntitlementsPolicy sent = Opa.NamespaceEntitlementsPolicy.newBuilder()
                .setNamespace("someone-elses-tenant")
                .build();

        AtomicReference<Throwable> error = new AtomicReference<>();
        publishAs(topic, tokenWithNamespace(CALLER_NAMESPACE), sent, error);
        assertNull(error.get(), "publish should have succeeded: " + error.get());

        KafkaSubscriber.Record record = readFirst(topic);
        assertNotNull(record, "the publish should have reached the log");

        Opa.NamespaceEntitlementsPolicy stored =
                Opa.NamespaceEntitlementsPolicy.parseFrom(record.value());
        assertEquals(CALLER_NAMESPACE, stored.getNamespace(),
                "the stored row must carry the caller's namespace claim, not the publisher's value");
        assertEquals(CALLER_NAMESPACE, record.headers().get("caller.namespace"),
                "the claim must also propagate as a Kafka header");
    }

    /**
     * The posture this whole change is about: no namespace claim on the token
     * is a refusal, not a pass-through and not an empty stamp. Letting the
     * publisher's own value stand would make the tenant boundary something the
     * publisher chooses.
     */
    @Test
    void publishWithoutNamespaceClaim_isRefusedPermissionDenied() throws Exception {
        String topic = "test.namespacestamp.Refused";
        topicManager.ensureTopicExists(topic);

        Opa.NamespaceEntitlementsPolicy sent = Opa.NamespaceEntitlementsPolicy.newBuilder()
                .setNamespace("someone-elses-tenant")
                .build();

        AtomicReference<Throwable> error = new AtomicReference<>();
        publishAs(topic, tokenWithoutNamespace(), sent, error);

        Throwable t = error.get();
        assertNotNull(t, "a publish with no namespace claim must fail the stream");
        assertEquals(Status.Code.PERMISSION_DENIED,
                ((StatusRuntimeException) t).getStatus().getCode(),
                "the refusal must be PERMISSION_DENIED; got " + t);
        assertNull(readFirst(topic),
                "the refused publish must not have reached the log");
    }

    /**
     * Negative control, so the refusal above is attributable to the stamper and
     * not to the claimless token: the SAME token on a topic whose type declares
     * no namespace field (namespaceStamper null, what the generator emits for
     * such a type) publishes normally.
     */
    @Test
    void topicWithoutNamespaceField_claimlessTokenStillPublishes() throws Exception {
        String topic = "test.namespacestamp.NoField";
        topicManager.ensureTopicExists(topic);

        Opa.NamespaceEntitlementsPolicy sent = Opa.NamespaceEntitlementsPolicy.newBuilder()
                .setNamespace("untouched")
                .build();

        AtomicReference<Throwable> error = new AtomicReference<>();
        publish(topic, tokenWithoutNamespace(), sent, null, error);
        assertNull(error.get(), "a topic with no namespace field must be unaffected: " + error.get());

        KafkaSubscriber.Record record = readFirst(topic);
        assertNotNull(record, "the publish should have reached the log");
        assertEquals("untouched",
                Opa.NamespaceEntitlementsPolicy.parseFrom(record.value()).getNamespace(),
                "with no stamper the message must reach the log exactly as sent");
    }

    // ---- helpers ----

    private static TokenWrapper tokenWithNamespace(String namespace) throws Exception {
        TokenWrapper token = baseToken().setNamespace(namespace);
        token.sign(signingKey);
        return token;
    }

    /**
     * A verified token carrying no namespace claim.
     *
     * <p>It can no longer be produced by the local mint path: the regenerated
     * {@code TokenWrapper.sign()} refuses to sign a token whose namespace claim
     * is absent. It can still ARRIVE as one — {@link TokenWrapper#parse} is the
     * wire path and imposes no such requirement — so this mints it the way a
     * foreign issuer delivers it. Whether that path should stay reachable at
     * all, and therefore whether PublishHandler's claimless refusal is live code
     * or dead code, is an open design question deliberately left unanswered
     * here: Q4 of workers/big8-eventbus-regen-namespace-2-questions.md.
     */
    private static TokenWrapper tokenWithoutNamespace() {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString(
                "{\"alg\":\"EdDSA\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = b64.encodeToString(
                ("{\"iss\":\"test-issuer\",\"aud\":\"eventbus\",\"sub_type\":\"service\""
                        + ",\"sub\":\"" + CALLER_SUB + "\""
                        + ",\"exp\":" + (System.currentTimeMillis() / 1000 + 3600) + "}")
                        .getBytes(StandardCharsets.UTF_8));
        String signature = b64.encodeToString("not-checked-here".getBytes(StandardCharsets.UTF_8));
        return TokenWrapper.parse(header + "." + payload + "." + signature);
    }

    private static TokenWrapper baseToken() {
        return TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience("eventbus")
                .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                .setSubType("service")
                .setSubject(CALLER_SUB);
    }

    /** Publishes through a handler wired the way the generator wires a topic
     *  whose type declares the namespace field. */
    private static void publishAs(String topic, TokenWrapper token,
                                  Opa.NamespaceEntitlementsPolicy message,
                                  AtomicReference<Throwable> error) throws Exception {
        publish(topic, token, message,
                (msg, namespace) -> msg.toBuilder().setNamespace(namespace).build(),
                error);
    }

    /**
     * Runs one publish through the real handler inside a gRPC context whose
     * verified-token slot holds the given token — the same slot the chassis
     * Authorizer fills on a live call.
     */
    private static void publish(String topic, TokenWrapper token,
                                Opa.NamespaceEntitlementsPolicy message,
                                PublishHandler.NamespaceStamper<Opa.NamespaceEntitlementsPolicy> stamper,
                                AtomicReference<Throwable> error) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<Eventbus.PublishResponse> resp = new StreamObserver<>() {
            @Override public void onNext(Eventbus.PublishResponse value) { }
            @Override public void onError(Throwable t) { error.set(t); done.countDown(); }
            @Override public void onCompleted() { done.countDown(); }
        };

        Context ctx = Auth.withVerifiedTokenSlot(Context.current());
        ctx.run(() -> {
            Auth.recordVerifiedToken(token);
            PublishHandler<Opa.NamespaceEntitlementsPolicy> handler = new PublishHandler<>(
                    topic, publisher, topicManager, resp,
                    null, null, null, null, null, stamper);
            handler.onNext(message);
            handler.onCompleted();
        });
        assertTrue(done.await(30, TimeUnit.SECONDS), "publish stream did not terminate");
    }

    /** The first record on a topic, or null if none arrives within 10s. */
    private static KafkaSubscriber.Record readFirst(String topic) throws Exception {
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        KafkaSubscriber.SeekParams params = new KafkaSubscriber.SeekParams(
                KafkaSubscriber.SeekMode.EARLIEST, 0, 0, null,
                false, KafkaSubscriber.BackpressurePolicy.BUFFER, 1, List.of());
        KafkaSubscriber.Session session = subscriber.createSession(topic, params);
        try {
            AtomicReference<KafkaSubscriber.Record> received = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            Thread.startVirtualThread(() -> session.run(params, record -> {
                received.set(record);
                latch.countDown();
                return false;
            }));
            latch.await(10, TimeUnit.SECONDS);
            return received.get();
        } finally {
            session.close();
        }
    }
}
