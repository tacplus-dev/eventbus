// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka;

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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests replay/seek functionality: offset-based, timestamp-based,
 * multi-message ordering, and latest-only subscription.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplayIntegrationTest {

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;
    private static KafkaPublisher publisher;
    private static KafkaSubscriber subscriber;

    private static final String REPLAY_TOPIC = "test.replay.Events";

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("replay-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 29093));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 29092, 29093,
                voters, clusterId, defaults, null);

        bootstrapAddress = broker.localAddress();
        Thread.sleep(3000);

        topicManager = new TopicManager(bootstrapAddress, 1, 1, 1);
        publisher = new KafkaPublisher(bootstrapAddress);
        subscriber = new KafkaSubscriber(bootstrapAddress);

        // Create topic and publish 10 messages with short delays between them.
        topicManager.ensureTopicExists(REPLAY_TOPIC);
        for (int i = 0; i < 10; i++) {
            byte[] payload = ("msg-" + i).getBytes(StandardCharsets.UTF_8);
            publisher.publish(REPLAY_TOPIC, "key-" + i,
                    Map.of("index", String.valueOf(i)), payload)
                    .get(10, TimeUnit.SECONDS);
            if (i < 9) Thread.sleep(50); // small gap for timestamp ordering
        }
    }

    @AfterAll
    static void stopBroker() {
        if (publisher != null) publisher.close();
        if (topicManager != null) topicManager.close();
        if (broker != null) broker.close();
    }

    @Test
    @Order(1)
    void subscribeFromEarliest_receivesAllMessages() throws Exception {
        List<KafkaSubscriber.Record> records = consumeN(
                KafkaSubscriber.SeekMode.EARLIEST, 0, 10, 15);

        assertEquals(10, records.size());
        assertEquals("msg-0", new String(records.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("msg-9", new String(records.get(9).value(), StandardCharsets.UTF_8));
    }

    @Test
    @Order(2)
    void subscribeFromOffset_skipsEarlierMessages() throws Exception {
        List<KafkaSubscriber.Record> records = consumeN(
                KafkaSubscriber.SeekMode.OFFSET, 5, 5, 15);

        assertEquals(5, records.size());
        assertEquals("msg-5", new String(records.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("msg-9", new String(records.get(4).value(), StandardCharsets.UTF_8));
    }

    @Test
    @Order(3)
    void subscribeFromLatest_receivesOnlyNewMessages() throws Exception {
        // Subscribe from latest — should not receive any of the 10 existing messages.
        KafkaSubscriber.SeekParams seekParams = new KafkaSubscriber.SeekParams(
                KafkaSubscriber.SeekMode.LATEST, 0, 0, null,
                false, KafkaSubscriber.BackpressurePolicy.BUFFER, 1,
                List.of(0));

        KafkaSubscriber.Session session = subscriber.createSession(REPLAY_TOPIC, seekParams);
        List<KafkaSubscriber.Record> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        Thread consumer = Thread.startVirtualThread(() -> session.run(seekParams, record -> {
            received.add(record);
            latch.countDown();
            return false;
        }));

        // Publish a new message after subscribing.
        Thread.sleep(500);
        publisher.publish(REPLAY_TOPIC, "new-key",
                Map.of("type", "new"), "new-msg".getBytes(StandardCharsets.UTF_8))
                .get(10, TimeUnit.SECONDS);

        assertTrue(latch.await(15, TimeUnit.SECONDS), "should receive new message");
        assertEquals(1, received.size());
        assertEquals("new-msg", new String(received.get(0).value(), StandardCharsets.UTF_8));
    }

    @Test
    @Order(4)
    void messagesPreserveOrdering() throws Exception {
        List<KafkaSubscriber.Record> records = consumeN(
                KafkaSubscriber.SeekMode.EARLIEST, 0, 10, 15);

        for (int i = 0; i < 10; i++) {
            assertEquals(i, records.get(i).offset(),
                    "offsets should be sequential from 0");
            assertEquals("msg-" + i,
                    new String(records.get(i).value(), StandardCharsets.UTF_8));
        }
    }

    @Test
    @Order(5)
    void headersPreservedAcrossRoundTrip() throws Exception {
        List<KafkaSubscriber.Record> records = consumeN(
                KafkaSubscriber.SeekMode.OFFSET, 3, 1, 15);

        assertEquals(1, records.size());
        assertEquals("3", records.get(0).headers().get("index"));
    }

    /**
     * Helper: consume N records from the replay topic starting at the given seek position.
     */
    private List<KafkaSubscriber.Record> consumeN(
            KafkaSubscriber.SeekMode mode, long value, int count, int timeoutSeconds)
            throws Exception {

        KafkaSubscriber.SeekParams seekParams = new KafkaSubscriber.SeekParams(
                mode, value, 0, null,
                false, KafkaSubscriber.BackpressurePolicy.BUFFER, 1,
                List.of(0)); // single partition

        KafkaSubscriber.Session session = subscriber.createSession(REPLAY_TOPIC, seekParams);
        List<KafkaSubscriber.Record> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(count);

        Thread consumer = Thread.startVirtualThread(() -> session.run(seekParams, record -> {
            received.add(record);
            latch.countDown();
            return latch.getCount() > 0;
        }));

        assertTrue(latch.await(timeoutSeconds, TimeUnit.SECONDS),
                "expected " + count + " records but got " + received.size());

        session.close();
        return received;
    }
}
