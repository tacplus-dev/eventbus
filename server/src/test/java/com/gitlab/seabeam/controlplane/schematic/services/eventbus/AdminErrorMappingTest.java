// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus;

import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.KafkaEmbeddedBroker;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.KafkaSubscriber;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.TopicManager;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Every Admin RPC used to answer every Kafka failure with INTERNAL. That broke
 * stratum's startup replay: ColdStartRebuilder.awaitWatermarks reads NOT_FOUND
 * from GetTopicInfo as "nothing to replay", and on anything else keeps its hot
 * reads closed -- which is what happened live when the eventbus volume came
 * back empty.
 *
 * <p>The first group pins {@link EventBusHandler#adminError} on the exception
 * shape TopicManager produces (RuntimeException over ExecutionException over
 * the Kafka error). The second group gets that shape from a real embedded
 * broker for each Admin RPC's TopicManager call, so the mapping is proven
 * against what Kafka actually throws, not what it is assumed to throw.
 */
class AdminErrorMappingTest {

    private static KafkaEmbeddedBroker broker;
    private static TopicManager topicManager;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("admin-error-mapping-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39893));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39892, 39893,
                voters, clusterId, defaults, null);
        Thread.sleep(3000);

        topicManager = new TopicManager(broker.localAddress(), 1, 1, 1);
    }

    @AfterAll
    static void stopBroker() {
        if (topicManager != null) topicManager.close();
        if (broker != null) broker.close();
    }

    // ---- the mapping, on TopicManager's wrapping ----

    @Test
    void unknownTopicOrPartition_isNotFound() {
        Exception e = new RuntimeException("Failed to get topic info for agentsensor.event_stream",
                new ExecutionException(new UnknownTopicOrPartitionException("This server does not host this topic-partition.")));
        assertEquals(ProtoException.NOT_FOUND, EventBusHandler.adminError(e).getCode());
    }

    @Test
    void anyOtherFailure_isInternal() {
        Exception e = new RuntimeException("Failed to get topic info for agentsensor.event_stream",
                new ExecutionException(new org.apache.kafka.common.errors.TimeoutException("Timed out waiting for a node assignment.")));
        assertEquals(ProtoException.INTERNAL, EventBusHandler.adminError(e).getCode());
        assertEquals(ProtoException.INTERNAL,
                EventBusHandler.adminError(new IllegalStateException("broker closed")).getCode());
    }

    // ---- what the real broker throws, per Admin RPC ----

    @Test
    void getTopicInfo_onAMissingTopic_isNotFound() {
        assertEquals(ProtoException.NOT_FOUND,
                codeOf(() -> topicManager.getTopicInfo("admin.error.mapping.Missing")));
    }

    @Test
    void getTopicInfo_onAnInvalidTopicName_isInvalidArgument() {
        assertEquals(ProtoException.INVALID_ARGUMENT,
                codeOf(() -> topicManager.getTopicInfo("")));
    }

    @Test
    void resetConsumerGroupOffsets_onAMissingTopic_isNotFound() {
        assertEquals(ProtoException.NOT_FOUND,
                codeOf(() -> topicManager.resetConsumerGroupOffsets(
                        "admin-error-mapping-group", "admin.error.mapping.Missing",
                        KafkaSubscriber.SeekMode.EARLIEST, 0)));
    }

    @Test
    void resetConsumerGroupOffsets_onAnInvalidTopicName_isInvalidArgument() {
        assertEquals(ProtoException.INVALID_ARGUMENT,
                codeOf(() -> topicManager.resetConsumerGroupOffsets(
                        "admin-error-mapping-group", "",
                        KafkaSubscriber.SeekMode.EARLIEST, 0)));
    }

    @Test
    void deleteConsumerGroup_onAMissingGroup_isNotFound() {
        assertEquals(ProtoException.NOT_FOUND,
                codeOf(() -> topicManager.deleteConsumerGroup("admin-error-mapping-no-such-group")));
    }

    private static int codeOf(Runnable topicManagerCall) {
        RuntimeException e = assertThrows(RuntimeException.class, topicManagerCall::run);
        return EventBusHandler.adminError(e).getCode();
    }
}
