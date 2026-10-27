// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus;

import com.github.tacplusdev.eventbus.generated.TypedEventBusBindings;
import com.github.tacplusdev.eventbus.model.chassis.grpcserver.Authorizer;
import com.github.tacplusdev.eventbus.model.chassis.key.ImmutableKey;
import com.github.tacplusdev.eventbus.model.chassis.key.KeyHelpers;
import com.github.tacplusdev.eventbus.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.eventbus.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.eventbus.model.types.sys.jwk.JwkOuterClass;
import com.github.tacplusdev.eventbus.model.types.sys.key.KeyOuterClass;
import com.github.tacplusdev.eventbus.model.types.sys.trace.Trace;

import com.google.protobuf.Message;

import io.grpc.CallOptions;
import io.grpc.ClientInterceptors;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F11: caller identity claims must come from a VERIFIED token.
 *
 * <p>{@link com.github.tacplusdev.eventbus.grpc.PublishHandler}
 * stamps {@code caller.sub}, {@code caller.aud} … onto every Kafka record from
 * {@code Auth.tokenPayloadFromContext()}. That used to be a raw base64 decode of
 * whatever bearer the caller sent, which proves only that the caller can write
 * base64. It is now the payload of the token the chassis Authorizer verified,
 * and is empty when nothing verified one.
 *
 * <p>These are real calls over a real gRPC socket against the real generated
 * per-topic bindings and the real chassis Authorizer, with real Ed25519-signed
 * tokens. The claims are observed at the innermost interceptor — the same
 * context state PublishHandler's constructor reads a moment later — so no Kafka
 * broker is needed to assert what identity a publish would have been stamped with.
 */
class ClaimsProvenanceTest {

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

    /**
     * A valid, correctly-signed, eventbus-audience S2S token carrying an `act`
     * claim (item 6 Piece A's delegated-mint actor). {@code setCustomField} is
     * used rather than a typed {@code setAct} because this checkout's generated
     * {@code TokenWrapper} predates the new claim's template accessor — the
     * payload map entry it writes is exactly what a regenerated {@code setAct}
     * would also write, so this is a faithful stand-in for extraction purposes.
     */
    private static String serviceTokenWithActor(ImmutableKey key, String service, String actorSpiffeId)
            throws Exception {
        TokenWrapper tok = TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience(AUDIENCE)
                .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                .setSubType("service")
                .setSubject(service)
                .setNamespace(NAMESPACE)
                .setCustomField("act", actorSpiffeId);
        tok.sign(key);
        return "Bearer " + tok.getBase64();
    }

    /**
     * A FORGED bearer: three base64url segments, a hand-written payload naming
     * someone else, and a signature that is not a signature. Nothing signed it.
     */
    private static String forgedToken(String claimedSub) {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString("{\"alg\":\"EdDSA\",\"kid\":\"no-such-key\"}".getBytes());
        String payload = b64.encodeToString(
                ("{\"sub\":\"" + claimedSub + "\",\"aud\":\"" + AUDIENCE + "\",\"iss\":\"nobody\"}").getBytes());
        String signature = b64.encodeToString("not-a-signature".getBytes());
        return "Bearer " + header + "." + payload + "." + signature;
    }

    // ---- server wiring ----

    /**
     * Records the claims visible to handler code — the exact value
     * PublishHandler's constructor reads — from the innermost position on the
     * interceptor stack.
     */
    private static ServerInterceptor claimsRecorder(AtomicReference<String> seen) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                seen.set(Auth.tokenPayloadFromContext());
                return Contexts.interceptCall(io.grpc.Context.current(), call, headers, next);
            }
        };
    }

    /**
     * Starts a server carrying the real per-topic definitions.
     *
     * <p>{@code withEnforcement} true is production: ServiceManager wraps
     * everything Handler.additionalServices() returns in enforcingInterceptor().
     * False is the hazard this finding is about — a definition that carries
     * requestIdInterceptor but reaches handler code with nothing having verified
     * a signature.
     */
    private static Server startServer(Authorizer authorizer, boolean withEnforcement,
                                      AtomicReference<String> seen) throws Exception {
        ServerBuilder<?> builder = ServerBuilder.forPort(0);
        for (ServerServiceDefinition perTopic : TypedEventBusBindings.buildPerTopicLazy(
                () -> null, () -> null, () -> null, traceId -> { })) {
            // Last interceptor runs first, so the recorder (listed first) runs last.
            ServerServiceDefinition svc = ServerInterceptors.intercept(
                    perTopic, claimsRecorder(seen), Auth.requestIdInterceptor());
            if (withEnforcement) {
                svc = ServerInterceptors.intercept(svc, authorizer.enforcingInterceptor());
            }
            builder.addService(svc);
        }
        return builder.build().start();
    }

    /**
     * Opens the bidi stream, half-closes, and returns the terminating Status.
     * OK is impossible here: without Kafka the generated body fails the stream
     * UNAVAILABLE, so the status tells us whether the call reached the body.
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

    private static final Message TRACE_EVENT = Trace.TraceEvent.getDefaultInstance();

    private static void shutdown(Server server) throws Exception {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }

    // ---- the claims a publish would be stamped with ----

    @Test
    void forgedToken_stampsNoCallerIdentity() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        AtomicReference<String> seen = new AtomicReference<>();
        // No enforcement in front: handler code is reached with nothing having
        // verified a signature. This is the shape F11 is about — the claims must
        // be empty, not the attacker's.
        Server server = startServer(authorizer, false, seen);
        try {
            call(server, forgedToken("someone-else"), PUBLISH_TRACE, TRACE_EVENT);
            assertNotNull(seen.get(), "handler code was never reached");
            assertEquals("", seen.get(),
                    "an unverified bearer must contribute no caller claims; got " + seen.get());
        } finally {
            shutdown(server);
        }
    }

    @Test
    void verifiedToken_stampsItsOwnClaims() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        AtomicReference<String> seen = new AtomicReference<>();
        Server server = startServer(authorizer, true, seen);
        try {
            call(server, serviceToken(key, "agentsensor"), PUBLISH_TRACE, TRACE_EVENT);
            assertNotNull(seen.get(), "handler code was never reached");
            assertTrue(seen.get().contains("\"agentsensor\""),
                    "a verified caller's own sub must still be stamped; got " + seen.get());
        } finally {
            shutdown(server);
        }
    }

    @Test
    void forgedToken_isRejectedByProductionWiring() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        AtomicReference<String> seen = new AtomicReference<>();
        Server server = startServer(authorizer, true, seen);
        try {
            Status s = call(server, forgedToken("someone-else"), PUBLISH_TRACE, TRACE_EVENT);
            assertEquals(Status.Code.UNAUTHENTICATED, s.getCode(),
                    "the ServiceManager's enforcingInterceptor must reject an unsigned bearer; got " + s);
        } finally {
            shutdown(server);
        }
    }

    /**
     * Item 6 Piece A: a verified token's `act` claim (the delegated-mint
     * actor) must be visible in the same verified-payload seam PublishHandler
     * reads — i.e. CLAIM_KEYS propagation is not special-cased away for `act`.
     */
    @Test
    void verifiedTokenWithActor_stampsActClaim() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        AtomicReference<String> seen = new AtomicReference<>();
        Server server = startServer(authorizer, true, seen);
        try {
            call(server, serviceTokenWithActor(key, "agentsensor", "spiffe://schematic/user/alice"),
                    PUBLISH_TRACE, TRACE_EVENT);
            assertNotNull(seen.get(), "handler code was never reached");
            assertTrue(seen.get().contains("\"act\":\"spiffe://schematic/user/alice\""),
                    "a verified caller's act claim must be visible to PublishHandler; got " + seen.get());
        } finally {
            shutdown(server);
        }
    }

    /**
     * The ordinary, non-delegated case: no `act` claim on the token means
     * PublishHandler's callerHeaders never carries `caller.act`, and nothing
     * downstream stamps an actor_spiffe_id.
     */
    @Test
    void verifiedTokenWithoutActor_noActClaim() throws Exception {
        ImmutableKey key = newTestKey();
        Authorizer authorizer = authorizerWithKey(key);
        AtomicReference<String> seen = new AtomicReference<>();
        Server server = startServer(authorizer, true, seen);
        try {
            call(server, serviceToken(key, "agentsensor"), PUBLISH_TRACE, TRACE_EVENT);
            assertNotNull(seen.get(), "handler code was never reached");
            assertTrue(!seen.get().contains("\"act\""),
                    "an ordinary token must carry no act claim; got " + seen.get());
        } finally {
            shutdown(server);
        }
    }
}

