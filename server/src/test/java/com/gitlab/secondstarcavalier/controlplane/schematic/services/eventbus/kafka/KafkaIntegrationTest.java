// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test: starts a real embedded KRaft broker, publishes a message,
 * subscribes, and verifies the round-trip.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaIntegrationTest {

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("integration-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 19093));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 19092, 19093,
                voters, clusterId, defaults, null);

        bootstrapAddress = broker.localAddress();

        // Wait for broker to be ready.
        Thread.sleep(3000);
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    @Order(1)
    void topicManager_createAndListTopics() {
        TopicManager tm = new TopicManager(bootstrapAddress, 1, 1, 1);
        try {
            tm.ensureTopicExists("test.integration.Topic1");
            List<TopicManager.TopicInfo> topics = tm.listTopics();

            boolean found = topics.stream()
                    .anyMatch(t -> t.name().equals("test.integration.Topic1"));
            assertTrue(found, "created topic should appear in list");
        } finally {
            tm.close();
        }
    }

    @Test
    @Order(2)
    void publishAndSubscribe_roundTrip() throws Exception {
        String topic = "test.integration.RoundTrip";
        byte[] payload = "hello-eventbus".getBytes(StandardCharsets.UTF_8);

        TopicManager tm = new TopicManager(bootstrapAddress, 1, 1, 1);
        KafkaPublisher publisher = new KafkaPublisher(bootstrapAddress);
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);

        try {
            // Ensure topic exists.
            tm.ensureTopicExists(topic);

            // Publish a message.
            var publishFuture = publisher.publish(topic, "key1",
                    Map.of("test-header", "test-value"), payload);
            var metadata = publishFuture.get(10, TimeUnit.SECONDS);
            assertTrue(metadata.offset() >= 0, "publish should return valid offset");

            // Subscribe from earliest and consume the message.
            KafkaSubscriber.SeekParams seekParams = new KafkaSubscriber.SeekParams(
                    KafkaSubscriber.SeekMode.EARLIEST, 0, 0, null,
                    false, KafkaSubscriber.BackpressurePolicy.BUFFER, 1, List.of());

            KafkaSubscriber.Session session = subscriber.createSession(topic, seekParams);
            AtomicReference<KafkaSubscriber.Record> received = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            Thread.startVirtualThread(() -> session.run(seekParams, record -> {
                received.set(record);
                latch.countDown();
                return false; // stop after first record
            }));

            assertTrue(latch.await(15, TimeUnit.SECONDS), "should receive message within 15s");

            KafkaSubscriber.Record record = received.get();
            assertNotNull(record);
            assertArrayEquals(payload, record.value());
            assertEquals("key1", record.key());
            assertEquals("test-value", record.headers().get("test-header"));
        } finally {
            publisher.close();
            tm.close();
        }
    }

    @Test
    @Order(3)
    void topicManager_getTopicInfo() {
        TopicManager tm = new TopicManager(bootstrapAddress, 1, 1, 1);
        try {
            tm.ensureTopicExists("test.integration.InfoTopic");
            TopicManager.TopicInfo info = tm.getTopicInfo("test.integration.InfoTopic");

            assertEquals("test.integration.InfoTopic", info.name());
            assertEquals(1, info.partitionCount());
            assertTrue(info.retentionMs() > 0, "retention should be positive");
        } finally {
            tm.close();
        }
    }

    @Test
    @Order(4)
    void topicManager_clusterHealth() {
        TopicManager tm = new TopicManager(bootstrapAddress, 1, 1, 1);
        try {
            TopicManager.ClusterHealth health = tm.getClusterHealth();
            assertEquals(1, health.brokerCount());
            assertFalse(health.brokers().isEmpty());
            assertEquals(1, health.brokers().get(0).id());
        } finally {
            tm.close();
        }
    }

    @Test
    @Order(5)
    void topicManager_deleteTopic() {
        TopicManager tm = new TopicManager(bootstrapAddress, 1, 1, 1);
        try {
            tm.ensureTopicExists("test.integration.DeleteMe");
            tm.deleteTopic("test.integration.DeleteMe");

            // Verify it's gone.
            List<TopicManager.TopicInfo> topics = tm.listTopics();
            boolean found = topics.stream()
                    .anyMatch(t -> t.name().equals("test.integration.DeleteMe"));
            assertFalse(found, "deleted topic should not appear in list");
        } finally {
            tm.close();
        }
    }
}
