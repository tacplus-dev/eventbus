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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Item 4: eventbus S2S subscribe is pinned to the subscriber's own tenant.
 *
 * <p>{@link PublishHandler} already stamps (spec.store_namespace) from the
 * caller's token, so every row on the log carries its owning tenant. Nothing
 * read it back: {@link SubscribeHandler} bound the consumer group to the
 * caller's sub but filtered nothing, so every tenant instance of a
 * tenant-owned service subscribing to a shared topic (docmanager's
 * DocFileMetadata, say) received every OTHER tenant's events too. Consumer
 * groups never collided — this was data exposure, not a routing bug.
 *
 * <p>Real embedded Kafka, real {@link PublishHandler} and {@link SubscribeHandler},
 * real Ed25519-signed tokens in the same verified-token context slot the chassis
 * Authorizer fills on a live call. Both tenants' rows are published to ONE topic
 * and read back off the broker through the real handler, so the assertions are on
 * what was actually delivered to the stream, not on what the handler was asked to do.
 *
 * <p>The three arms are deliberately shaped so the negative one cannot pass
 * vacuously: {@link #sameTenantEvent_isStillDelivered} proves the harness
 * delivers at all, {@link #systemSubscriber_stillSeesEveryTenant} proves the
 * fleet-wide read is untouched, and
 * {@link #topicWithoutNamespaceField_isUnfiltered} proves a drop is attributable
 * to the filter rather than to the token or the topic.
 *
 * <p>{@code opa.NamespaceEntitlementsPolicy} stands in for a record type, the
 * same stand-in {@link NamespaceStampTest} uses: an ordinary generated message
 * with a string {@code namespace} field, so {@code ::getNamespace} is the very
 * reader the generator emits for a real topic.
 */
class SubscribeTenantFilterTest {

    private static final String ACME = "/tenants/acme";
    private static final String BETA = "/tenants/beta";
    /** A shared platform service: the fleet, not a tenant. */
    private static final String SYSTEM = "/system";

    private static final String ACME_SUB = "spiffe://test/tenants/acme/services/docmanager";
    private static final String BETA_SUB = "spiffe://test/tenants/beta/services/docmanager";
    private static final String SYSTEM_SUB = "spiffe://test/system/services/stratum";

    /** The read half the generator emits for a type declaring the namespace field. */
    private static final Function<Opa.NamespaceEntitlementsPolicy, String> NAMESPACE_READER =
            Opa.NamespaceEntitlementsPolicy::getNamespace;

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;
    private static KafkaPublisher publisher;
    private static ImmutableKey signingKey;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("subscribe-tenant-filter-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39693));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39692, 39693,
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

    // ---- the pin ----

    /**
     * The gap this item closes. Both tenants' rows sit on one topic; the acme
     * subscriber must see its own and never beta's.
     */
    @Test
    void tenantSubscriber_neverSeesAnotherTenantsEvent() throws Exception {
        String topic = "test.subscribetenant.CrossTenant";
        publishBothTenants(topic);

        List<String> delivered = subscribeAndDrain(topic, ACME_SUB, ACME, NAMESPACE_READER);

        assertEquals(List.of(ACME), delivered,
                "the acme subscriber must receive its own row and nothing of beta's; got " + delivered);
    }

    /**
     * The positive arm, so the negative one above is the filter firing rather
     * than the harness failing to deliver anything at all.
     */
    @Test
    void sameTenantEvent_isStillDelivered() throws Exception {
        String topic = "test.subscribetenant.SameTenant";
        topicManager.ensureTopicExists(topic);
        publishAs(topic, ACME);

        List<String> delivered = subscribeAndDrain(topic, ACME_SUB, ACME, NAMESPACE_READER);

        assertEquals(List.of(ACME), delivered,
                "a subscriber's own tenant's event must still be delivered; got " + delivered);
    }

    /**
     * Fleet-wide read unchanged: a /system caller is a shared platform service
     * and reads every tenant, which is what stratum's ingestion depends on.
     */
    @Test
    void systemSubscriber_stillSeesEveryTenant() throws Exception {
        String topic = "test.subscribetenant.FleetWide";
        publishBothTenants(topic);

        List<String> delivered = subscribeAndDrain(topic, SYSTEM_SUB, SYSTEM, NAMESPACE_READER);

        Collections.sort(delivered);
        assertEquals(List.of(ACME, BETA), delivered,
                "a /system subscriber must still see every tenant; got " + delivered);
    }

    /**
     * Negative control on the OTHER axis: the same tenant token on a topic whose
     * type declares no namespace field (namespaceReader null, what the generator
     * emits for such a type) is unfiltered. So a drop above is attributable to
     * the reader being wired, not to the token being a tenant token.
     */
    @Test
    void topicWithoutNamespaceField_isUnfiltered() throws Exception {
        String topic = "test.subscribetenant.NoField";
        publishBothTenants(topic);

        List<String> delivered = subscribeAndDrain(topic, ACME_SUB, ACME, null);

        Collections.sort(delivered);
        assertEquals(List.of(ACME, BETA), delivered,
                "a topic with no namespace field must stay fleet-wide for everyone; got " + delivered);
    }

    /**
     * Fail closed. On a topic that declares the field, a caller that proved no
     * tenant is refused rather than served fleet-wide — the same posture
     * PublishHandler takes on exactly the same set of topics.
     */
    @Test
    void subscribeWithoutNamespaceClaim_isRefusedPermissionDenied() throws Exception {
        String topic = "test.subscribetenant.Claimless";
        topicManager.ensureTopicExists(topic);

        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        AtomicReference<Throwable> error = new AtomicReference<>();
        SubscribeHandler<Opa.NamespaceEntitlementsPolicy> handler = new SubscribeHandler<>(
                subscriber, topic, collectingObserver(new ArrayList<>(), error),
                Opa.NamespaceEntitlementsPolicy::parseFrom, NAMESPACE_READER);

        Context ctx = Auth.withVerifiedTokenSlot(Context.current());
        ctx.run(() -> {
            Auth.recordVerifiedToken(tokenWithoutNamespace());
            handler.onNext(Eventbus.SubscribeRequest.newBuilder().setEarliest(true).build());
        });

        Throwable t = error.get();
        assertNotNull(t, "a subscribe with no namespace claim must fail the stream");
        assertEquals(Status.Code.PERMISSION_DENIED,
                ((StatusRuntimeException) t).getStatus().getCode(),
                "the refusal must be PERMISSION_DENIED; got " + t);
    }

    /** The claim-shape rule itself, stated once. */
    @Test
    void namespacePinned_isTrueForTenantsAndFalseForSystem() {
        assertTrue(SubscribeHandler.namespacePinned(ACME));
        assertTrue(SubscribeHandler.namespacePinned(BETA));
        assertTrue(!SubscribeHandler.namespacePinned(SYSTEM));
        assertTrue(!SubscribeHandler.namespacePinned("/system/services/stratum"));
    }

    // ---- helpers ----

    /** Both tenants' rows on ONE topic, which is the shape that exposed them. */
    private static void publishBothTenants(String topic) throws Exception {
        topicManager.ensureTopicExists(topic);
        publishAs(topic, ACME);
        publishAs(topic, BETA);
    }

    /**
     * Publishes one row through the real handler as the given tenant, letting the
     * generated stamper stamp the namespace from the token — so the row's field
     * is populated exactly the way production populates it.
     */
    private static void publishAs(String topic, String namespace) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        StreamObserver<Eventbus.PublishResponse> resp = new StreamObserver<>() {
            @Override public void onNext(Eventbus.PublishResponse value) { }
            @Override public void onError(Throwable t) { error.set(t); done.countDown(); }
            @Override public void onCompleted() { done.countDown(); }
        };

        Context ctx = Auth.withVerifiedTokenSlot(Context.current());
        ctx.run(() -> {
            Auth.recordVerifiedToken(tokenFor(namespace));
            PublishHandler<Opa.NamespaceEntitlementsPolicy> handler = new PublishHandler<>(
                    topic, publisher, topicManager, resp,
                    null, null, null, null, null,
                    (msg, ns) -> msg.toBuilder().setNamespace(ns).build(),
                    NAMESPACE_READER);
            handler.onNext(Opa.NamespaceEntitlementsPolicy.getDefaultInstance());
            handler.onCompleted();
        });
        assertTrue(done.await(30, TimeUnit.SECONDS), "publish stream did not terminate");
        assertNull(error.get(), "publish should have succeeded: " + error.get());
    }

    /**
     * Subscribes from EARLIEST through the real handler as the given identity and
     * returns the namespace of every event actually delivered to the stream.
     *
     * <p>Everything is published before the subscribe starts, so a full drain is
     * a fixed settle window rather than a count: waiting for N deliveries would
     * make the cross-tenant assertion pass simply by never seeing the row it is
     * supposed to prove absent.
     */
    private static List<String> subscribeAndDrain(String topic, String sub, String namespace,
            Function<Opa.NamespaceEntitlementsPolicy, String> namespaceReader) throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> error = new AtomicReference<>();

        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        SubscribeHandler<Opa.NamespaceEntitlementsPolicy> handler = new SubscribeHandler<>(
                subscriber, topic, collectingObserver(delivered, error),
                Opa.NamespaceEntitlementsPolicy::parseFrom, namespaceReader);

        Context ctx = Auth.withVerifiedTokenSlot(Context.current());
        ctx.run(() -> {
            Auth.recordVerifiedToken(tokenFor(namespace, sub));
            assertEquals(namespace, Auth.callerNamespaceFromContext(),
                    "test context should carry the caller namespace claim");
            handler.onNext(Eventbus.SubscribeRequest.newBuilder().setEarliest(true).build());
        });

        Thread.sleep(15_000);
        // Client cancel: closes the session and stops the consumer.
        handler.onError(new RuntimeException("test done"));

        assertNull(error.get(), "subscribe should not have failed: " + error.get());
        return new ArrayList<>(delivered);
    }

    /**
     * Collects the namespace out of each delivered envelope. The handler emits
     * the SubscribeRecord wrapper, so the payload is unwrapped and parsed the way
     * a real client parses it.
     */
    private static StreamObserver<DynamicMessage> collectingObserver(
            List<String> delivered, AtomicReference<Throwable> error) {
        Descriptors.FieldDescriptor payloadField = SubscribeHandler.subscribeRecordDefaultInstance()
                .getDescriptorForType().findFieldByName("payload");
        return new StreamObserver<>() {
            @Override public void onNext(DynamicMessage value) {
                try {
                    ByteString payload = (ByteString) value.getField(payloadField);
                    delivered.add(Opa.NamespaceEntitlementsPolicy.parseFrom(payload).getNamespace());
                } catch (Exception e) {
                    error.set(e);
                }
            }
            @Override public void onError(Throwable t) { error.set(t); }
            @Override public void onCompleted() { }
        };
    }

    private static TokenWrapper tokenFor(String namespace) {
        return tokenFor(namespace, ACME.equals(namespace) ? ACME_SUB : BETA_SUB);
    }

    private static TokenWrapper tokenFor(String namespace, String sub) {
        try {
            TokenWrapper token = TokenWrapper.create()
                    .setIssuer("test-issuer")
                    .setAudience("eventbus")
                    .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                    .setSubType("service")
                    .setSubject(sub)
                    .setNamespace(namespace);
            token.sign(signingKey);
            return token;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A verified token carrying no namespace claim, minted the way a foreign
     * issuer delivers it — {@code TokenWrapper.sign()} refuses to produce one.
     * Same construction as {@link NamespaceStampTest}.
     */
    private static TokenWrapper tokenWithoutNamespace() {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString(
                "{\"alg\":\"EdDSA\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = b64.encodeToString(
                ("{\"iss\":\"test-issuer\",\"aud\":\"eventbus\",\"sub_type\":\"service\""
                        + ",\"sub\":\"" + ACME_SUB + "\""
                        + ",\"exp\":" + (System.currentTimeMillis() / 1000 + 3600) + "}")
                        .getBytes(StandardCharsets.UTF_8));
        String signature = b64.encodeToString("not-checked-here".getBytes(StandardCharsets.UTF_8));
        return TokenWrapper.parse(header + "." + payload + "." + signature);
    }
}
