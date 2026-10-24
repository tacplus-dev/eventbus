// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus;

import com.gitlab.seabeam.controlplane.schematic.services.eventbus.generated.TypedEventBusBindings;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.grpcserver.Authorizer;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.key.ImmutableKey;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.key.KeyHelpers;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Auth;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.token.TokenWrapper;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.jwk.JwkOuterClass;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.key.KeyOuterClass;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.trace.Trace;

import com.google.protobuf.Message;

import io.grpc.CallOptions;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * F15: eventbus topics are authenticated but not authorized.
 *
 * <p>The ServiceManager wraps everything Handler.additionalServices() returns in
 * Authorizer.enforcingInterceptor(), which verifies signature/audience/token
 * class but explicitly does NOT check entitlements. Every per-topic
 * ServerServiceDefinition was registered that way, so any holder of an
 * eventbus-audience token could publish to and subscribe from ANY topic —
 * including the platform trace topic.
 *
 * <p>These are real calls over a real gRPC socket against the real generated
 * bindings and the real chassis Authorizer, with real Ed25519-signed tokens.
 * Kafka is deliberately absent: a call that clears authorization reaches the
 * generated handler body and comes back UNAVAILABLE("Kafka not yet
 * initialized"), which is what distinguishes "allowed through" from "denied".
 *
 * <p>{@link #negativeControl_withoutTopicAuthorization_theBadCallerGetsThrough}
 * is the guard against a vacuous suite: it rebuilds the pre-fix wiring
 * (enforcingInterceptor only) and asserts the SAME un-entitled caller is NOT
 * denied. If the denial assertions ever started passing for an unrelated
 * reason, that test would fail.
 */
class TopicAuthorizationTest {

    private static final String AUDIENCE = "eventbus";
    private static final String NAMESPACE = "/local/test";

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    // ---- token minting (same seam the generated AuthorizerBehaviorTest uses) ----

    private static ImmutableKey newTestKey() throws Exception {
        KeyOuterClass.Key proto = KeyHelpers.newEd25519Key();
        return ImmutableKey.from(proto);
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

    /** A valid, correctly-signed, eventbus-audience S2S token for the given service. */
    private static String serviceToken(ImmutableKey key, String service) throws Exception {
        TokenWrapper tok = TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience(AUDIENCE)
                .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                .setSubType("service")
                .setSubject(service)
                // Mint-time requirement as of the regenerated TokenWrapper: sign()
                // refuses a token carrying no namespace claim.
                .setNamespace(NAMESPACE);
        tok.sign(key);
        return "Bearer " + tok.getBase64();
    }

    /** A valid eventbus-audience USER token — the C2S leg, carrying no entitlements. */
    private static String userToken(ImmutableKey key) throws Exception {
        TokenWrapper tok = TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience(AUDIENCE)
                .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                .setSubType("user")
                .setSubject("someone@example.com")
                .setNamespace(NAMESPACE);
        tok.sign(key);
        return "Bearer " + tok.getBase64();
    }

    // ---- server wiring ----

    /**
     * Starts a server carrying the real per-topic definitions, wired exactly as
     * production does it. With {@code withTopicAuthorization} false this is the
     * PRE-FIX wiring: the ServiceManager's enforcingInterceptor and nothing else.
     */
    private static Server startServer(Authorizer authorizer, boolean withTopicAuthorization)
            throws Exception {
        ServerBuilder<?> builder = ServerBuilder.forPort(0);
        if (withTopicAuthorization) {
            TypedEventBusBindings.registerMethodAuth(authorizer);
        }
        ServerInterceptor topicAuth = authorizer.authorizingInterceptor();
        for (ServerServiceDefinition perTopic : TypedEventBusBindings.buildPerTopicLazy(
                () -> null, () -> null, () -> null, traceId -> { })) {
            ServerServiceDefinition svc = withTopicAuthorization
                    ? ServerInterceptors.intercept(perTopic, Auth.requestIdInterceptor(), topicAuth)
                    : ServerInterceptors.intercept(perTopic, Auth.requestIdInterceptor());
            // What ServiceManager.registerServices does to every additionalServices() entry.
            builder.addService(ServerInterceptors.intercept(svc, authorizer.enforcingInterceptor()));
        }
        return builder.build().start();
    }

    /**
     * Opens the bidi stream, half-closes, and returns the terminating Status.
     * OK is impossible here: without Kafka the generated body fails the stream
     * UNAVAILABLE, so the status tells us whether the call reached the body at
     * all.
     */
    private static Status call(Server server, String bearer,
                               MethodDescriptor<Message, Object> method, Message request)
            throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        try {
            Metadata headers = new Metadata();
            if (bearer != null) {
                headers.put(AUTHORIZATION, bearer);
            }
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
            assertTrue(done.await(15, TimeUnit.SECONDS), "call did not terminate");
            return status.get();
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private static MethodDescriptor<Message, Object> erase(MethodDescriptor<?, ?> md) {
        return (MethodDescriptor<Message, Object>) md;
    }

    private static final MethodDescriptor<Message, Object> PUBLISH_TRACE =
            erase(TypedEventBusBindings.PUBLISH_TO_TRACE_EVENT_BUS_TRACE_EVENTS);
    private static final MethodDescriptor<Message, Object> SUBSCRIBE_TRACE =
            erase(TypedEventBusBindings.SUBSCRIBE_TO_TRACE_EVENT_BUS_TRACE_EVENTS);
    private static final MethodDescriptor<Message, Object> SUBSCRIBE_LETS_ENCRYPT_CERT =
            erase(TypedEventBusBindings.SUBSCRIBE_TO_LETS_ENCRYPT_CERT_BUS_STREAM);

    private static final Message TRACE_EVENT = Trace.TraceEvent.getDefaultInstance();
    private static final Message SUBSCRIBE_REQUEST = Eventbus.SubscribeRequest.getDefaultInstance();

    // ---- REJECTION: a valid eventbus token without the topic's entitlement ----

    @Test
    void publishToTraceTopic_withoutEntitlement_isDenied() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        Server server = startServer(authorizer, true);
        try {
            // "tasks" holds a perfectly valid eventbus-audience token. The trace
            // topic's auth_services is ["eventbus"], so it must not get in.
            Status s = call(server, serviceToken(key, "tasks"), PUBLISH_TRACE, TRACE_EVENT);
            assertEquals(Status.Code.PERMISSION_DENIED, s.getCode(),
                    "tasks must not publish to the trace topic; got " + s);
            assertTrue(s.getDescription() != null
                            && s.getDescription().contains("PublishToTraceEvents"),
                    "denial should name the method: " + s.getDescription());
        } finally {
            shutdown(server);
        }
    }

    @Test
    void subscribeToTraceTopic_withoutEntitlement_isDenied() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        Server server = startServer(authorizer, true);
        try {
            // Reading the platform trace topic is the sharpest form of the leak:
            // it carries every other service's request traces.
            Status s = call(server, serviceToken(key, "gateway"), SUBSCRIBE_TRACE, SUBSCRIBE_REQUEST);
            assertEquals(Status.Code.PERMISSION_DENIED, s.getCode(),
                    "gateway must not subscribe to the trace topic; got " + s);
        } finally {
            shutdown(server);
        }
    }

    @Test
    void userTokenWithoutEntitlements_isDenied() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        Server server = startServer(authorizer, true);
        try {
            // The C2S leg: a valid user token carrying no service_entitlements.
            Status s = call(server, userToken(key), SUBSCRIBE_TRACE, SUBSCRIBE_REQUEST);
            assertEquals(Status.Code.PERMISSION_DENIED, s.getCode(),
                    "a user token with no entitlements must be denied; got " + s);
        } finally {
            shutdown(server);
        }
    }

    // ---- POSITIVE: the entitled caller still gets through ----

    @Test
    void publishToTraceTopic_withEntitlement_isAllowedThrough() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        Server server = startServer(authorizer, true);
        try {
            Status s = call(server, serviceToken(key, "eventbus"), PUBLISH_TRACE, TRACE_EVENT);
            // Authorization passed, so the call reached the generated body, which
            // reports UNAVAILABLE because no Kafka is wired in this test.
            assertEquals(Status.Code.UNAVAILABLE, s.getCode(),
                    "eventbus must reach the publish handler; got " + s);
            assertTrue(s.getDescription() != null && s.getDescription().contains("Kafka"),
                    "expected the handler's own UNAVAILABLE: " + s.getDescription());
        } finally {
            shutdown(server);
        }
    }

    /**
     * The LetsEncryptCert topic's SUBSCRIBE policy is ["stratum"], from the
     * record's (spec.record).s2s_subscribe. Both halves are asserted here because
     * the producing service losing its automatic read is the entire point of the
     * declaration: before it existed, SubscribeToStream carried no policy of its
     * own and fell back to the synthesized LetsEncryptCertBus wrapper's
     * service-level list, which resolves to the OWNING PACKAGE. That gave
     * letsencrypt — the publisher — a read it was never meant to have, and
     * denied stratum, the record store, the only read the topic exists to serve.
     */
    @Test
    void subscribeToRecordTopic_onlyTheDeclaredSubscriberGetsThrough() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        Server server = startServer(authorizer, true);
        try {
            Status store = call(server, serviceToken(key, "stratum"), SUBSCRIBE_LETS_ENCRYPT_CERT, SUBSCRIBE_REQUEST);
            assertEquals(Status.Code.UNAVAILABLE, store.getCode(),
                    "stratum is the declared subscriber and must reach the handler; got " + store);

            Status producer = call(server, serviceToken(key, "letsencrypt"), SUBSCRIBE_LETS_ENCRYPT_CERT, SUBSCRIBE_REQUEST);
            assertEquals(Status.Code.PERMISSION_DENIED, producer.getCode(),
                    "letsencrypt publishes this topic but must not read it back; got " + producer);
        } finally {
            shutdown(server);
        }
    }

    // ---- SAME RPC NAME, DIFFERENT TOPIC: policies must not bleed across ----

    /**
     * Two topics that share a bare rpc name (every record field named "stream"
     * yields a PublishToStream) and declare different auth_services must keep
     * their policies apart. Keyed by bare rpc name — as the chassis Authorizer
     * did before this change — one policy overwrote the other, so a caller
     * entitled on one topic was either refused there or admitted on the other.
     *
     * <p>The regression needs such a pair in the generated bindings. This fleet
     * declares one topic per bare rpc name, so the test is skipped naming what
     * is missing; when a fleet gains the pair it fails instead, because the
     * assertions have to be written against that pair's concrete types.
     */
    @Test
    void sameRpcNameOnTwoTopics_policiesStayApart() {
        Map<String, List<String>> byBareRpc = new HashMap<>();
        for (ServerServiceDefinition perTopic : TypedEventBusBindings.buildPerTopicLazy(
                () -> null, () -> null, () -> null, traceId -> { })) {
            for (ServerMethodDefinition<?, ?> method : perTopic.getMethods()) {
                String full = method.getMethodDescriptor().getFullMethodName();
                byBareRpc.computeIfAbsent(MethodDescriptor.extractBareMethodName(full),
                        bare -> new ArrayList<>()).add(full);
            }
        }
        List<List<String>> shared = byBareRpc.values().stream()
                .filter(fullNames -> fullNames.size() > 1)
                .toList();
        assumeFalse(shared.isEmpty(),
                "fleet declares no two topics sharing a bare rpc name (rpcs: " + byBareRpc.keySet()
                        + "); the same-rpc-name regression needs such a pair");
        fail("fleet now carries topics sharing a bare rpc name " + shared
                + "; write this test's fixture against that pair");
    }

    // ---- NEGATIVE CONTROL: the check is what does the rejecting ----

    @Test
    void negativeControl_withoutTopicAuthorization_theBadCallerGetsThrough() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        // Pre-fix wiring: enforcingInterceptor only — authenticated, not authorized.
        Server server = startServer(authorizer, false);
        try {
            Status s = call(server, serviceToken(key, "tasks"), PUBLISH_TRACE, TRACE_EVENT);
            assertNotEquals(Status.Code.PERMISSION_DENIED, s.getCode(),
                    "without the topic check the un-entitled caller must NOT be denied — "
                            + "if this fails, the denial tests above prove nothing");
            assertEquals(Status.Code.UNAVAILABLE, s.getCode(),
                    "pre-fix, tasks reaches the trace topic's publish handler; got " + s);
        } finally {
            shutdown(server);
        }
    }

    private static void shutdown(Server server) throws Exception {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }
}
