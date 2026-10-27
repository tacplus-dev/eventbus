// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus;

import com.github.tacplusdev.eventbus.generated.TopicRegistry;
import com.github.tacplusdev.eventbus.generated.TypedEventBusBindings;
import com.github.tacplusdev.eventbus.kafka.KafkaEmbeddedBroker;
import com.github.tacplusdev.eventbus.kafka.KafkaPublisher;
import com.github.tacplusdev.eventbus.kafka.KafkaSubscriber;
import com.github.tacplusdev.eventbus.kafka.TopicManager;
import com.github.tacplusdev.eventbus.model.chassis.grpcserver.Authorizer;
import com.github.tacplusdev.eventbus.model.chassis.key.ImmutableKey;
import com.github.tacplusdev.eventbus.model.chassis.key.KeyHelpers;
import com.github.tacplusdev.eventbus.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.eventbus.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.eventbus.model.types.sys.jwk.JwkOuterClass;
import com.github.tacplusdev.eventbus.model.types.sys.key.KeyOuterClass;
import com.github.tacplusdev.eventbus.model.types.services.letsencrypt.Letsencrypt;
import com.github.tacplusdev.eventbus.model.types.sys.trace.Trace;

import com.google.protobuf.Message;

import io.grpc.CallOptions;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * (spec.store_namespace) at publish ingest, on the fleet's OWN record types and
 * through the generator's own wiring.
 *
 * <p>{@link NamespaceStampTest} pins the same behaviour against a hand-written
 * stamper lambda and {@code opa.NamespaceEntitlementsPolicy} standing in for a
 * record, because at the time it was written no fleet record type carried the
 * field in this checkout's generated model. They do now, so this asserts the
 * thing that actually ships: {@code letsencrypt.LetsEncryptCert} published
 * over a real gRPC socket, through the real chassis {@link Authorizer}, into the
 * per-topic {@link ServerServiceDefinition} that
 * {@link TypedEventBusBindings#buildPerTopicLazy} builds, with the stamper the
 * generator emitted — read back off the broker and parsed.
 *
 * <p>The pair is a real one, not a constructed one: {@code LetsEncryptCert} is
 * a {@code (spec.record)} and declares {@code (spec.store_namespace)}, so its
 * binding carries a stamper; {@code sys.trace.TraceEvent} is the fleet's one
 * topic type that is not a record and declares no namespace field, so its
 * binding carries {@code null} there. Same server, same token, same broker —
 * only the declaration differs.
 */
class FleetRecordNamespaceStampTest {

    private static final String AUDIENCE = "eventbus";
    private static final String CALLER_SERVICE = "letsencrypt";
    private static final String CALLER_NAMESPACE = "/local/acme";
    /** The tenant a publishing service writes a row on behalf of — never its own. */
    private static final String ON_BEHALF_OF_NAMESPACE = "/local/someone-elses-tenant";

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;
    private static KafkaPublisher publisher;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("fleet-record-namespace-stamp-test");
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
    }

    @AfterAll
    static void stopBroker() {
        if (publisher != null) publisher.close();
        if (topicManager != null) topicManager.close();
        if (broker != null) broker.close();
    }

    // ---- the stamp, on the record type the fleet actually publishes ----

    /**
     * The paired control for the three-way stamp
     * (the upstream namespace-fix decision of 2026-09-02, not carried here),
     * both halves on one topic, one server and one token so the ONLY thing that
     * differs between them is whether the row arrived with its
     * (spec.store_namespace) field set.
     *
     * <p>ROW SET: the publishing service tenanted the row itself, from identity
     * it authenticated in its own request context — a write made on someone
     * else's behalf. That value stands, which is the only way such a row can
     * ever be read by the person it belongs to.
     *
     * <p>ROW EMPTY: a write the service made purely on its own behalf. The
     * caller's own verified namespace claim is stamped on, exactly as before
     * this behaviour existed.
     */
    @Test
    void letsEncryptCert_keepsARowSetNamespace_andStampsAnEmptyOne() throws Exception {
        ImmutableKey key = newTestKey();
        Server server = startServer(authorizerWithKey(key));
        try {
            Letsencrypt.LetsEncryptCert delegated = Letsencrypt.LetsEncryptCert.newBuilder()
                    .setCertSetId("cert-set-1")
                    .setCertmagicState(Letsencrypt.CertMagicState.getDefaultInstance())
                    .addHostnames("one.example.test")
                    .setNamespace(ON_BEHALF_OF_NAMESPACE)
                    .build();
            Status delegatedStatus = call(server, serviceToken(key),
                    erase(TypedEventBusBindings.PUBLISH_TO_LETS_ENCRYPT_CERT_BUS_STREAM), delegated);
            assertEquals(Status.Code.OK, delegatedStatus.getCode(),
                    "publish should have succeeded; got " + delegatedStatus);

            Letsencrypt.LetsEncryptCert ownBehalf = Letsencrypt.LetsEncryptCert.newBuilder()
                    .setCertSetId("cert-set-2")
                    .setCertmagicState(Letsencrypt.CertMagicState.getDefaultInstance())
                    .addHostnames("two.example.test")
                    .build();
            Status ownStatus = call(server, serviceToken(key),
                    erase(TypedEventBusBindings.PUBLISH_TO_LETS_ENCRYPT_CERT_BUS_STREAM), ownBehalf);
            assertEquals(Status.Code.OK, ownStatus.getCode(),
                    "publish should have succeeded; got " + ownStatus);

            List<KafkaSubscriber.Record> records =
                    readFirst(TopicRegistry.TOPIC_LETS_ENCRYPT_CERT_BUS_STREAM, 2);
            assertEquals(2, records.size(), "both publishes should have reached the log");
            assertEquals(ON_BEHALF_OF_NAMESPACE,
                    Letsencrypt.LetsEncryptCert.parseFrom(records.get(0).value()).getNamespace(),
                    "a row the publishing service already tenanted must keep that tenant");
            assertEquals(CALLER_NAMESPACE,
                    Letsencrypt.LetsEncryptCert.parseFrom(records.get(1).value()).getNamespace(),
                    "a row that arrived with no tenant must carry the caller's namespace claim");
        } finally {
            shutdown(server);
        }
    }

    /**
     * The other half of the pair: a fleet topic whose type declares no namespace
     * field. The generator emits a null stamper for it, so the same caller on
     * the same server publishes through untouched.
     */
    @Test
    void traceEvent_declaresNoNamespaceField_andIsUntouched() throws Exception {
        ImmutableKey key = newTestKey();
        Server server = startServer(authorizerWithKey(key));
        try {
            Trace.TraceEvent sent = Trace.TraceEvent.newBuilder()
                    .setTraceId("trace-1")
                    .setRound(Trace.TraceRound.newBuilder().setService(CALLER_SERVICE))
                    .build();

            Status status = call(server, serviceToken(key),
                    erase(TypedEventBusBindings.PUBLISH_TO_TRACE_EVENT_BUS_TRACE_EVENTS), sent);
            assertEquals(Status.Code.OK, status.getCode(), "publish should have succeeded; got " + status);

            KafkaSubscriber.Record record = readFirst(TopicRegistry.TOPIC_TRACE_EVENT_BUS_TRACE_EVENTS);
            assertNotNull(record, "the publish should have reached the log");
            assertEquals(sent, Trace.TraceEvent.parseFrom(record.value()),
                    "with no namespace field the message must reach the log exactly as sent");
        } finally {
            shutdown(server);
        }
    }

    // ---- token minting (same seam ClaimsProvenanceTest uses) ----

    private static ImmutableKey newTestKey() throws Exception {
        return ImmutableKey.from(KeyHelpers.newEd25519Key());
    }

    /** The verifying JWK for a signing key; the raw Ed25519 key is the SPKI tail. */
    private static JwkOuterClass.Jwk jwkForKey(ImmutableKey key) {
        byte[] spki = key.getPublicKey().getEncoded();
        byte[] raw = new byte[32];
        System.arraycopy(spki, spki.length - 32, raw, 0, 32);
        return JwkOuterClass.Jwk.newBuilder()
                .setKid(key.getProto().getId())
                .setKty(KeyOuterClass.KeyType.KEY_TYPE_OKP)
                .setCrv("Ed25519")
                .setX(Base64.getUrlEncoder().withoutPadding().encodeToString(raw))
                .build();
    }

    private static Authorizer authorizerWithKey(ImmutableKey key) {
        Map<String, JwkOuterClass.Jwk> entries = new HashMap<>();
        JwkOuterClass.Jwk jwk = jwkForKey(key);
        entries.put(jwk.getKid(), jwk);
        return new Authorizer(sink -> sink.update(entries));
    }

    private static String serviceToken(ImmutableKey key) throws Exception {
        TokenWrapper tok = TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience(AUDIENCE)
                .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                .setSubType("service")
                .setSubject(CALLER_SERVICE)
                .setNamespace(CALLER_NAMESPACE);
        tok.sign(key);
        return "Bearer " + tok.getBase64();
    }

    // ---- server wiring ----

    /** Production shape: the per-topic definitions behind the enforcing interceptor. */
    private static Server startServer(Authorizer authorizer) throws Exception {
        ServerBuilder<?> builder = ServerBuilder.forPort(0);
        for (ServerServiceDefinition perTopic : TypedEventBusBindings.buildPerTopicLazy(
                () -> publisher, () -> null, () -> topicManager, traceId -> { })) {
            builder.addService(ServerInterceptors.intercept(
                    ServerInterceptors.intercept(perTopic, Auth.requestIdInterceptor()),
                    authorizer.enforcingInterceptor()));
        }
        return builder.build().start();
    }

    private static void shutdown(Server server) throws Exception {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private static MethodDescriptor<Message, Object> erase(MethodDescriptor<?, ?> md) {
        return (MethodDescriptor<Message, Object>) md;
    }

    /** Opens the bidi stream, sends one message, half-closes, returns the terminating Status. */
    private static Status call(Server server, String bearer,
                               MethodDescriptor<Message, Object> method, Message request)
            throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        try {
            Metadata headers = new Metadata();
            headers.put(AUTHORIZATION, bearer);
            io.grpc.Channel authed = ClientInterceptors.intercept(
                    channel, MetadataUtils.newAttachHeadersInterceptor(headers));

            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Status> status = new AtomicReference<>();
            StreamObserver<Message> req = ClientCalls.asyncBidiStreamingCall(
                    authed.newCall(method, CallOptions.DEFAULT),
                    new StreamObserver<Object>() {
                        @Override public void onNext(Object value) { }
                        @Override public void onError(Throwable t) {
                            status.set(t instanceof StatusRuntimeException
                                    ? ((StatusRuntimeException) t).getStatus()
                                    : Status.fromThrowable(t));
                            done.countDown();
                        }
                        @Override public void onCompleted() {
                            status.set(Status.OK);
                            done.countDown();
                        }
                    });
            req.onNext(request);
            req.onCompleted();
            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("call did not terminate");
            }
            return status.get();
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** The first record on a topic, or null if none arrives within 10s. */
    private static KafkaSubscriber.Record readFirst(String topic) throws Exception {
        List<KafkaSubscriber.Record> records = readFirst(topic, 1);
        return records.isEmpty() ? null : records.get(0);
    }

    /** The first {@code n} records on a topic, in log order. */
    private static List<KafkaSubscriber.Record> readFirst(String topic, int n) throws Exception {
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        KafkaSubscriber.SeekParams params = new KafkaSubscriber.SeekParams(
                KafkaSubscriber.SeekMode.EARLIEST, 0, 0, null,
                false, KafkaSubscriber.BackpressurePolicy.BUFFER, n, List.of());
        KafkaSubscriber.Session session = subscriber.createSession(topic, params);
        try {
            List<KafkaSubscriber.Record> received =
                    java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            CountDownLatch latch = new CountDownLatch(n);
            Thread.startVirtualThread(() -> session.run(params, record -> {
                received.add(record);
                latch.countDown();
                return received.size() < n;
            }));
            latch.await(10, TimeUnit.SECONDS);
            return List.copyOf(received);
        } finally {
            session.close();
        }
    }
}
