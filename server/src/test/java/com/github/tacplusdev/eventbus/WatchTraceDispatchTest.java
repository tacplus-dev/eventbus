// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus;

import com.github.tacplusdev.eventbus.grpc.PublishHandler;
import com.github.tacplusdev.eventbus.grpc.WatchTraceHandler;
import com.github.tacplusdev.eventbus.kafka.KafkaEmbeddedBroker;
import com.github.tacplusdev.eventbus.kafka.KafkaPublisher;
import com.github.tacplusdev.eventbus.kafka.TopicManager;
import com.github.tacplusdev.eventbus.model.chassis.key.ImmutableKey;
import com.github.tacplusdev.eventbus.model.chassis.key.KeyHelpers;
import com.github.tacplusdev.eventbus.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.eventbus.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.eventbus.model.types.services.eventbus.Eventbus;
import com.github.tacplusdev.eventbus.model.types.services.letsencrypt.Letsencrypt;
import com.github.tacplusdev.eventbus.model.types.sys.trace.Trace;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Trace propagation on publish, the WatchTrace hop (tacplus-dev/looper#51,
 * #28): a published message's field-200 {@code trace_id} reaches the WatchTrace
 * session that owns it, as an internal-request TraceEvent carrying that
 * trace_id, and no other session. {@link com.github.tacplusdev.eventbus.grpc.PublishTraceIdTest}
 * covers the Kafka hop and stops at the dispatcher callback; this goes through
 * the real {@link EventBusHandler#dispatchTrace(WatchTraceHandler, String, String)}
 * into a real {@link WatchTraceHandler}, wired as the bindings wire it.
 */
class WatchTraceDispatchTest {

    private static KafkaEmbeddedBroker broker;
    private static TopicManager topicManager;
    private static KafkaPublisher publisher;
    private static ImmutableKey signingKey;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("watch-trace-dispatch-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39993));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39992, 39993,
                voters, clusterId, defaults, null);

        String bootstrapAddress = broker.localAddress();
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

    /**
     * Two WatchTrace sessions, A and B; one publish stream sending a message
     * traced with A's trace_id, an untraced one, and one with a trace_id no
     * session watches. A gets exactly one event after its first, with its
     * trace_id and kind INTERNAL_REQUEST; B gets nothing after its first.
     */
    @Test
    void aPublishsTraceIdReachesItsWatchTraceSession_andNoOther() throws Exception {
        String topic = "test.watchtracedispatch.Cert";
        topicManager.ensureTopicExists(topic);

        WatchTraceHandler watchTrace = new WatchTraceHandler();
        List<Trace.TraceEvent> sessionA = Collections.synchronizedList(new ArrayList<>());
        List<Trace.TraceEvent> sessionB = Collections.synchronizedList(new ArrayList<>());
        watchTrace.subscribeTrace(null, capturing(sessionA));
        watchTrace.subscribeTrace(null, capturing(sessionB));
        assertEquals(1, sessionA.size(), "a session's first event is its trace_id");
        assertEquals(1, sessionB.size(), "a session's first event is its trace_id");
        String traceA = sessionA.get(0).getTraceId();
        String traceB = sessionB.get(0).getTraceId();
        assertFalse(traceA.isEmpty(), "the first event must carry the session's trace_id");
        assertNotEquals(traceA, traceB, "each session gets its own trace_id");

        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<Eventbus.PublishResponse> resp = new StreamObserver<>() {
            @Override public void onNext(Eventbus.PublishResponse value) { }
            @Override public void onError(Throwable t) { error.set(t); done.countDown(); }
            @Override public void onCompleted() { done.countDown(); }
        };

        TokenWrapper token = TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience("eventbus")
                .setExpiration(System.currentTimeMillis() / 1000 + 3600)
                .setSubType("service")
                .setSubject("spiffe://test/system/services/letsencrypt")
                .setNamespace("acme");
        token.sign(signingKey);

        Context ctx = Auth.withVerifiedTokenSlot(Context.current());
        ctx.run(() -> {
            Auth.recordVerifiedToken(token);
            PublishHandler<Letsencrypt.LetsEncryptCert> handler = new PublishHandler<>(
                    topic, publisher, topicManager, resp,
                    null, Letsencrypt.LetsEncryptCert::getTraceId,
                    traceId -> EventBusHandler.dispatchTrace(watchTrace, "instance-1", traceId));
            handler.onNext(cert("traced", traceA));
            handler.onNext(cert("untraced", ""));
            handler.onNext(cert("unwatched", "no-such-session"));
            handler.onCompleted();
        });
        assertTrue(done.await(30, TimeUnit.SECONDS), "publish stream did not terminate");
        assertNull(error.get(), "publish should have succeeded: " + error.get());

        assertEquals(2, sessionA.size(),
                "session A must get exactly one event after its first: its publish's, and none for the untraced or unwatched ones");
        Trace.TraceEvent event = sessionA.get(1);
        assertEquals(traceA, event.getTraceId(), "the event must carry the publish's trace_id");
        assertEquals(Trace.TraceEventKind.TRACE_EVENT_KIND_INTERNAL_REQUEST, event.getKind());
        assertEquals("instance-1", event.getRound().getInstanceId());

        assertEquals(1, sessionB.size(),
                "session B must get nothing after its first: no publish carried its trace_id");
    }

    private static StreamObserver<Trace.TraceEvent> capturing(List<Trace.TraceEvent> events) {
        return new StreamObserver<>() {
            @Override public void onNext(Trace.TraceEvent value) { events.add(value); }
            @Override public void onError(Throwable t) { }
            @Override public void onCompleted() { }
        };
    }

    private static Letsencrypt.LetsEncryptCert cert(String certSetId, String traceId) {
        return Letsencrypt.LetsEncryptCert.newBuilder()
                .setCertSetId(certSetId)
                .setTraceId(traceId)
                .build();
    }
}
