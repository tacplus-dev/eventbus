// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.grpc;

import com.github.tacplusdev.eventbus.kafka.KafkaEmbeddedBroker;
import com.github.tacplusdev.eventbus.kafka.KafkaSubscriber;
import com.github.tacplusdev.eventbus.kafka.TopicManager;
import com.github.tacplusdev.eventbus.model.types.services.eventbus.Eventbus;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;

import io.grpc.stub.ServerCallStreamObserver;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Subscribe call the client half-closed and then cancelled must close its
 * session.
 *
 * <p>The generated Java subscriber half-closes right after its one
 * SubscribeRequest, and SubscribeHandler deliberately keeps streaming through
 * that half-close. grpc-java does not deliver the request observer's onError
 * for a cancel that arrives after a half-close, so before the cancel handler
 * the session -- and its Kafka consumer, still heartbeating and holding
 * partitions -- outlived the subscriber until the next record on one of its
 * partitions failed to send. That record then waited out a rebalance, 4-7s on
 * dev after every stratum roll.
 *
 * <p>The call observer here stands in for grpc's ServerCallStreamObserverImpl,
 * which is what the generated bindings hand SubscribeHandler at runtime; the
 * sessions are real ones against an embedded broker.
 */
class SubscribeCancelTest {

    private static final String TOPIC = "test.subscribecancel.Events";

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("subscribe-cancel-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(2, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39193));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39192, 39193,
                voters, clusterId, defaults, null);

        bootstrapAddress = broker.localAddress();
        Thread.sleep(3000);

        topicManager = new TopicManager(bootstrapAddress, 2, 1, 1);
        topicManager.ensureTopicExists(TOPIC);
    }

    @AfterAll
    static void stopBroker() {
        if (topicManager != null) topicManager.close();
        if (broker != null) broker.close();
    }

    @Test
    void halfClosedThenCancelled_closesTheSession() throws Exception {
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        FakeCall call = new FakeCall();
        SubscribeHandler<Empty> handler = new SubscribeHandler<>(subscriber, TOPIC, call, Empty::parseFrom);

        handler.onNext(Eventbus.SubscribeRequest.newBuilder().setEarliest(true).build());
        handler.onCompleted();
        assertTrue(awaitCondition(() -> subscriber.getSessionsActive() == 1),
                "the subscribe should be streaming after the half-close");

        call.cancel();

        assertTrue(awaitCondition(() -> subscriber.getSessionsActive() == 0),
                "a cancel after half-close must close the session, active_sessions="
                        + subscriber.getSessionsActive());
    }

    /**
     * The cancel lands before onNext has assigned the session -- in production,
     * while createSession is still blocked in its initial assignment poll. The
     * session onNext goes on to create must not be left running.
     */
    @Test
    void cancelledBeforeTheSessionExists_doesNotLeaveItRunning() throws Exception {
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        FakeCall call = new FakeCall();
        SubscribeHandler<Empty> handler = new SubscribeHandler<>(subscriber, TOPIC, call, Empty::parseFrom);

        call.cancel();
        handler.onNext(Eventbus.SubscribeRequest.newBuilder().setEarliest(true).build());

        assertTrue(awaitCondition(() -> subscriber.getSessionsStarted() == 1
                        && subscriber.getSessionsActive() == 0),
                "a session created for an already-cancelled call must end, started="
                        + subscriber.getSessionsStarted() + " active=" + subscriber.getSessionsActive());
    }

    private static boolean awaitCondition(BooleanSupplier condition) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(200);
        }
        return false;
    }

    /** Captures the cancel handler the way grpc's server call observer holds it. */
    private static final class FakeCall extends ServerCallStreamObserver<DynamicMessage> {
        private volatile Runnable onCancel;
        private volatile boolean cancelled;

        void cancel() {
            cancelled = true;
            Runnable r = onCancel;
            if (r != null) r.run();
        }

        @Override public void setOnCancelHandler(Runnable onCancelHandler) { this.onCancel = onCancelHandler; }
        @Override public boolean isCancelled() { return cancelled; }
        @Override public void onNext(DynamicMessage value) { }
        @Override public void onError(Throwable t) { }
        @Override public void onCompleted() { }
        @Override public void setCompression(String compression) { }
        @Override public boolean isReady() { return true; }
        @Override public void setOnReadyHandler(Runnable onReadyHandler) { }
        @Override public void disableAutoInboundFlowControl() { }
        @Override public void request(int count) { }
        @Override public void setMessageCompression(boolean enable) { }
    }
}
