// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the two consumer-group starvation modes observed on the
 * cluster after a stratum roll:
 *
 * 1. A subscription whose handler always fails must not stop the other
 *    subscriptions in the same consumer group from being dispatched — its
 *    failure, and the rebalance its departure causes, must stay local to it.
 * 2. A client-paused subscription must keep calling poll(). A pause loop that
 *    sleeps instead holds the group membership and the partition assignment
 *    without polling, so the coordinator evicts the member and the group stops
 *    consuming with no error anywhere — the "Stable but silently dead" mode.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConsumerGroupStarvationTest {

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;
    private static KafkaPublisher publisher;

    private static final String TOPIC = "test.starvation.Events";

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("starvation-test");
        // Two partitions so a group can hold two members at once.
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(2, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39093));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39092, 39093,
                voters, clusterId, defaults, null);

        bootstrapAddress = broker.localAddress();
        Thread.sleep(3000);

        topicManager = new TopicManager(bootstrapAddress, 2, 1, 1);
        publisher = new KafkaPublisher(bootstrapAddress);
        topicManager.ensureTopicExists(TOPIC);
    }

    @AfterAll
    static void stopBroker() {
        if (publisher != null) publisher.close();
        if (topicManager != null) topicManager.close();
        if (broker != null) broker.close();
    }

    private static void publishBatch(String prefix, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            publisher.publish(TOPIC, prefix + "-" + i, Map.of(),
                    (prefix + "-" + i).getBytes(StandardCharsets.UTF_8))
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static KafkaSubscriber.SeekParams params(String group, boolean pause) {
        return new KafkaSubscriber.SeekParams(
                KafkaSubscriber.SeekMode.EARLIEST, 0, 0, group,
                pause, KafkaSubscriber.BackpressurePolicy.BUFFER, 1, List.of());
    }

    /**
     * Mode 1: one subscription in the group always throws. The healthy
     * subscription in the same group must keep receiving records, and the group
     * must not end up with no members.
     */
    @Test
    @Order(1)
    void failingSubscriptionDoesNotStarveItsConsumerGroup() throws Exception {
        String group = "test.starvation.group1";
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);

        publishBatch("seed", 10);

        // Healthy subscription.
        List<String> healthyReceived = Collections.synchronizedList(new ArrayList<>());
        KafkaSubscriber.SeekParams healthyParams = params(group, false);
        KafkaSubscriber.Session healthy = subscriber.createSession(TOPIC, healthyParams);
        Thread.startVirtualThread(() -> healthy.run(healthyParams, record -> {
            healthyReceived.add(new String(record.value(), StandardCharsets.UTF_8));
            return true;
        }));
        assertTrue(awaitCondition(() -> !healthyReceived.isEmpty(), 30_000),
                "healthy subscription should receive the seed batch");

        // Failing subscription: joins the same group, throws on every record.
        // This is the UNIMPLEMENTED-style per-subscription failure — its join,
        // its failure and the LeaveGroup that follows all rebalance the group.
        AtomicInteger failingDispatches = new AtomicInteger();
        KafkaSubscriber.SeekParams failingParams = params(group, false);
        KafkaSubscriber.Session failing = subscriber.createSession(TOPIC, failingParams);
        Thread.startVirtualThread(() -> failing.run(failingParams, record -> {
            failingDispatches.incrementAndGet();
            throw new UnsupportedOperationException("UNIMPLEMENTED");
        }));

        // Enough records that both partitions certainly get some, so the failing
        // member is certain to be handed one to fail on.
        publishBatch("churn", 20);

        assertTrue(awaitCondition(() -> subscriber.getSessionsFailed() >= 1, 60_000),
                "failing subscription should terminate its own session, dispatches="
                        + failingDispatches.get());

        // The failing session is gone. Everything published now must still reach
        // the healthy subscription: if its poll loop had been starved by the
        // failure or wedged by the rebalance, nothing arrives.
        int before = healthyReceived.size();
        publishBatch("after", 10);
        assertTrue(awaitCondition(() -> healthyReceived.size() >= before + 10, 60_000),
                "healthy subscription should keep consuming after the failing one dies; "
                        + "received " + healthyReceived.size() + ", expected at least " + (before + 10));

        // ...and the group must still have a member.
        TopicManager.ConsumerGroupInfo info = topicManager.getConsumerGroupLag(group);
        assertEquals("Stable", info.state(),
                "group should still be Stable with the healthy member attached");

        assertEquals(0, subscriber.getPollStallsDetected(), "no poll stall should be reported");
        healthy.close();
    }

    /**
     * Mode 2: a paused subscription must keep polling. Sleeping instead leaves
     * the member registered but silent — the group reports members, the pod is
     * healthy, lag stays pinned, and only a restart clears it.
     */
    @Test
    @Order(2)
    void pausedSubscriptionKeepsPolling() throws Exception {
        String group = "test.starvation.group2";
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);

        publishBatch("paused-seed", 4);

        List<String> received = Collections.synchronizedList(new ArrayList<>());
        KafkaSubscriber.SeekParams pausedParams = params(group, true);
        KafkaSubscriber.Session session = subscriber.createSession(TOPIC, pausedParams);
        Thread.startVirtualThread(() -> session.run(pausedParams, record -> {
            received.add(new String(record.value(), StandardCharsets.UTF_8));
            return true;
        }));

        // Wait for the session to get its assignment and settle into the pause loop.
        assertTrue(awaitCondition(() -> subscriber.getSessionsAssigned() >= 1, 30_000),
                "paused session should still acquire a partition assignment");
        assertTrue(awaitCondition(() -> subscriber.getPollsCompleted() > 0, 30_000),
                "paused session should be polling");

        long pollsAtStart = subscriber.getPollsCompleted();
        Thread.sleep(2000);
        long pollsAfter = subscriber.getPollsCompleted();

        // At PAUSED_POLL_MS = 100ms this is ~20 polls in 2s. The old sleep loop
        // produced exactly zero, which is what got the member evicted.
        assertTrue(pollsAfter - pollsAtStart >= 5,
                "paused session must keep polling to hold its group membership; polls went from "
                        + pollsAtStart + " to " + pollsAfter);
        assertTrue(received.isEmpty(), "paused session must not deliver records");

        // The membership it kept is real: unpausing delivers without rejoining.
        assertEquals("Stable", topicManager.getConsumerGroupLag(group).state(),
                "paused member should still be in a Stable group");

        session.resubscribe(params(group, false));
        assertTrue(awaitCondition(() -> !received.isEmpty(), 30_000),
                "unpaused session should deliver records");

        assertEquals(0, subscriber.getPollStallsDetected(), "no poll stall should be reported");
        session.close();
    }

    /**
     * Mode 3: the poll loop itself is the thread that is stuck.
     *
     * This is the 2026-09-15 incident. Every stratum consumer group went Empty
     * on heartbeat expiration and stayed Empty for 25 hours while the gRPC
     * SubscribeToStream streams stayed open on both sides — so stratum's
     * reconnect loop never fired and nothing anywhere reported a problem.
     *
     * The thing that made it silent is that the poll loop was the stuck thread.
     * Any recovery that runs ON that thread — noticing a flag, catching a
     * WakeupException — cannot happen, because the thread is what is blocked.
     * So the stall watchdog must terminate the stream from its own thread, and
     * that is what this asserts: a handler that never returns still gets the
     * session failed, with the loop still parked inside it.
     */
    @Test
    @Order(3)
    void aStalledPollLoopFailsItsStreamFromTheWatchdogThread() throws Exception {
        String group = "test.starvation.group3";
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);

        publishBatch("stalled", 4);

        CountDownLatch dispatched = new CountDownLatch(1);
        CompletableFuture<String> failedStream = new CompletableFuture<>();
        KafkaSubscriber.SeekParams stalledParams = params(group, false);
        KafkaSubscriber.Session session = subscriber.createSession(TOPIC, stalledParams);
        // Stands in for SubscribeHandler's stream termination.
        session.onStalled(failedStream::complete);

        Thread loop = new Thread(() -> {
            try {
                session.run(stalledParams, record -> {
                    dispatched.countDown();
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return false;
                });
            } catch (RuntimeException expected) {
                // ConsumerStalledException once the loop is released at the end.
            }
        }, "stalled-session");
        loop.setDaemon(true);
        loop.start();

        assertTrue(dispatched.await(60, TimeUnit.SECONDS),
                "the handler should be dispatched a record before it blocks");

        // POLL_STALL_WARN_MS is 60s and the watchdog scans every 30s, so the
        // report lands between 60s and 90s after that last poll.
        String reason = failedStream.get(150, TimeUnit.SECONDS);
        assertTrue(reason.contains(TOPIC), "stall reason should name the topic: " + reason);
        assertTrue(subscriber.getPollStallsDetected() >= 1, "the stall must be counted");
        assertTrue(loop.isAlive(),
                "the point of the test: the stream was failed while the poll loop is still stuck");

        loop.interrupt();
        session.close();
    }

    private static boolean awaitCondition(BooleanSupplierEx condition, long timeoutMs)
            throws Exception {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.get()) return true;
            Thread.sleep(200);
        }
        return condition.get();
    }

    @FunctionalInterface
    private interface BooleanSupplierEx {
        boolean get() throws Exception;
    }
}
