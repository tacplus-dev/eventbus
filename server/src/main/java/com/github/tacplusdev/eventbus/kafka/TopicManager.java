// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.kafka;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AdminClient wrapper for topic management, consumer group administration,
 * and cluster health.
 * Auto-creates topics on first publish using configured defaults.
 */
public class TopicManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TopicManager.class);
    private final AdminClient admin;
    private final int defaultPartitions;
    private final int defaultReplicationFactor;
    private final long defaultRetentionMs;
    private final Set<String> knownTopics = new HashSet<>();

    public TopicManager(String bootstrapServers, int defaultPartitions,
                        int defaultReplicationFactor, int defaultRetentionHours) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = AdminClient.create(props);
        this.defaultPartitions = defaultPartitions;
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.defaultRetentionMs = defaultRetentionHours * 3600L * 1000L;
    }

    // -----------------------------------------------------------------------
    // Topic lifecycle
    // -----------------------------------------------------------------------

    /**
     * Ensures a topic exists, creating it with defaults if needed.
     */
    public void ensureTopicExists(String topic) {
        if (knownTopics.contains(topic)) return;

        try {
            Set<String> existing = admin.listTopics().names().get();
            if (existing.contains(topic)) {
                knownTopics.add(topic);
                return;
            }

            Map<String, String> config = Map.of(
                    "retention.ms", String.valueOf(defaultRetentionMs),
                    "compression.type", "lz4"
            );
            NewTopic newTopic = new NewTopic(topic, defaultPartitions, (short) defaultReplicationFactor)
                    .configs(config);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
            knownTopics.add(topic);
            logger.info("Created topic: {} (partitions={}, replication={}, retention={}h)",
                    topic, defaultPartitions, defaultReplicationFactor, defaultRetentionMs / 3600000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted creating topic " + topic, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                knownTopics.add(topic);
            } else {
                throw new RuntimeException("Failed to create topic " + topic, e);
            }
        }
    }

    /**
     * Deletes a topic.
     */
    public void deleteTopic(String topic) {
        try {
            admin.deleteTopics(Collections.singleton(topic)).all().get();
            knownTopics.remove(topic);
            logger.info("Deleted topic: {}", topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted deleting topic " + topic, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to delete topic " + topic, e);
        }
    }

    /**
     * Alters topic configuration. Only non-zero fields are applied.
     * Returns updated topic info.
     */
    public TopicInfo alterTopicConfig(String topic, long retentionMs, int partitionCount) {
        try {
            // Alter retention if specified.
            if (retentionMs != 0) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                String retentionValue = retentionMs == -1 ? "-1" : String.valueOf(retentionMs);
                ConfigEntry entry = new ConfigEntry("retention.ms", retentionValue);
                AlterConfigOp op = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
                admin.incrementalAlterConfigs(Map.of(resource, List.of(op))).all().get();
                logger.info("Altered topic {} retention to {}ms", topic, retentionMs);
            }

            // Increase partitions if specified.
            if (partitionCount > 0) {
                admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(partitionCount))).all().get();
                logger.info("Altered topic {} partitions to {}", topic, partitionCount);
            }

            return getTopicInfo(topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted altering topic " + topic, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to alter topic " + topic, e);
        }
    }

    /**
     * Returns metadata for a single topic.
     */
    public TopicInfo getTopicInfo(String topic) {
        try {
            DescribeTopicsResult result = admin.describeTopics(Collections.singleton(topic));
            TopicDescription desc = result.topicNameValues().get(topic).get();

            Map<Integer, Long> earliestOffsets = new HashMap<>();
            Map<Integer, Long> latestOffsets = new HashMap<>();

            List<TopicPartition> partitions = desc.partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            // Get earliest and latest offsets
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                    admin.listOffsets(partitions.stream().collect(Collectors.toMap(
                            tp -> tp, tp -> OffsetSpec.earliest()))).all().get();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                    admin.listOffsets(partitions.stream().collect(Collectors.toMap(
                            tp -> tp, tp -> OffsetSpec.latest()))).all().get();

            for (TopicPartition tp : partitions) {
                earliestOffsets.put(tp.partition(), earliest.get(tp).offset());
                latestOffsets.put(tp.partition(), latest.get(tp).offset());
            }

            // Get retention config
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config config = admin.describeConfigs(Collections.singleton(resource)).all().get().get(resource);
            long retentionMs = Long.parseLong(config.get("retention.ms").value());

            return new TopicInfo(topic, desc.partitions().size(), retentionMs, earliestOffsets, latestOffsets);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted getting topic info for " + topic, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to get topic info for " + topic, e);
        }
    }

    /**
     * Lists all topics with metadata.
     */
    public List<TopicInfo> listTopics() {
        try {
            Set<String> names = admin.listTopics().names().get();
            List<TopicInfo> infos = new ArrayList<>();
            for (String name : names) {
                if (name.startsWith("__")) continue; // skip internal topics
                infos.add(getTopicInfo(name));
            }
            return infos;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted listing topics", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to list topics", e);
        }
    }

    // -----------------------------------------------------------------------
    // Consumer group admin
    // -----------------------------------------------------------------------

    /**
     * Returns lag info for a consumer group across all its subscribed topics.
     */
    public ConsumerGroupInfo getConsumerGroupLag(String groupId) {
        try {
            // Get committed offsets for the group.
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata().get();

            // Get end offsets for the same partitions.
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(committed.keySet().stream().collect(Collectors.toMap(
                            tp -> tp, tp -> OffsetSpec.latest()))).all().get();

            // Get group state.
            ConsumerGroupDescription desc = admin.describeConsumerGroups(
                    Collections.singleton(groupId)).describedGroups().get(groupId).get();

            List<PartitionLag> partitionLags = new ArrayList<>();
            long totalLag = 0;

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                TopicPartition tp = entry.getKey();
                long committedOffset = entry.getValue().offset();
                long endOffset = endOffsets.get(tp).offset();
                long lag = Math.max(0, endOffset - committedOffset);
                totalLag += lag;
                partitionLags.add(new PartitionLag(
                        tp.topic(), tp.partition(), committedOffset, endOffset, lag));
            }

            return new ConsumerGroupInfo(
                    groupId, desc.state().toString(), partitionLags, totalLag);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted getting consumer group lag for " + groupId, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to get consumer group lag for " + groupId, e);
        }
    }

    /**
     * Of the given groups, the ones the coordinator reports as having no
     * members, with each one's total lag.
     *
     * Deliberately narrower than {@link #listConsumerGroups}: this runs on the
     * health-reporter thread every 10s, so it describes only the handful of
     * groups eventbus itself is subscribed to and does not fetch per-partition
     * offsets for every group in the cluster. The 5s deadline is half the
     * cluster probe's, because it is the third admin round trip that probe can
     * make and the health cadence is 10s.
     */
    public Map<String, Long> groupsWithNoMembers(Collection<String> groupIds) {
        if (groupIds.isEmpty()) return Map.of();
        try {
            Map<String, ConsumerGroupDescription> described =
                    admin.describeConsumerGroups(groupIds).all().get(5, TimeUnit.SECONDS);
            Map<String, Long> starved = new LinkedHashMap<>();
            for (Map.Entry<String, ConsumerGroupDescription> entry : described.entrySet()) {
                if (entry.getValue().members().isEmpty()) {
                    starved.put(entry.getKey(), totalLag(entry.getKey()));
                }
            }
            return starved;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted describing consumer groups", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to describe consumer groups", e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Consumer group membership probe timed out", e);
        }
    }

    /**
     * Total lag across a group's committed partitions, or -1 when it cannot be
     * read. Only used to put a number in the log line that reports a starved
     * group — the membership check above is what decides.
     */
    private long totalLag(String groupId) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            if (committed.isEmpty()) return 0;
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(committed.keySet().stream().collect(Collectors.toMap(
                            tp -> tp, tp -> OffsetSpec.latest()))).all().get(5, TimeUnit.SECONDS);
            long total = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                total += Math.max(0, endOffsets.get(entry.getKey()).offset() - entry.getValue().offset());
            }
            return total;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Lists all consumer groups with their lag info.
     */
    public List<ConsumerGroupInfo> listConsumerGroups() {
        try {
            Collection<ConsumerGroupListing> listings =
                    admin.listConsumerGroups().all().get();
            List<ConsumerGroupInfo> groups = new ArrayList<>();
            for (ConsumerGroupListing listing : listings) {
                groups.add(getConsumerGroupLag(listing.groupId()));
            }
            return groups;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted listing consumer groups", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to list consumer groups", e);
        }
    }

    /**
     * Resets consumer group offsets for a specific topic.
     * The group must be inactive (no active consumers).
     */
    public void resetConsumerGroupOffsets(String groupId, String topic,
                                          KafkaSubscriber.SeekMode mode, long value) {
        try {
            // Describe the topic to get partitions.
            TopicDescription desc = admin.describeTopics(
                    Collections.singleton(topic)).topicNameValues().get(topic).get();
            List<TopicPartition> partitions = desc.partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();

            switch (mode) {
                case OFFSET:
                    for (TopicPartition tp : partitions) {
                        newOffsets.put(tp, new OffsetAndMetadata(value));
                    }
                    break;
                case TIMESTAMP:
                    Map<TopicPartition, OffsetSpec> timestampSpecs = partitions.stream()
                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.forTimestamp(value)));
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                            admin.listOffsets(timestampSpecs).all().get();
                    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsets.entrySet()) {
                        newOffsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
                    }
                    break;
                case LATEST:
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                            admin.listOffsets(partitions.stream().collect(Collectors.toMap(
                                    tp -> tp, tp -> OffsetSpec.latest()))).all().get();
                    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : latestOffsets.entrySet()) {
                        newOffsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
                    }
                    break;
                case EARLIEST:
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                            admin.listOffsets(partitions.stream().collect(Collectors.toMap(
                                    tp -> tp, tp -> OffsetSpec.earliest()))).all().get();
                    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : earliestOffsets.entrySet()) {
                        newOffsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
                    }
                    break;
            }

            admin.alterConsumerGroupOffsets(groupId, newOffsets).all().get();
            logger.info("Reset consumer group {} offsets for topic {} (mode={}, value={})",
                    groupId, topic, mode, value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted resetting offsets for " + groupId, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to reset offsets for " + groupId, e);
        }
    }

    /**
     * Deletes a consumer group. The group must be inactive.
     */
    public void deleteConsumerGroup(String groupId) {
        try {
            admin.deleteConsumerGroups(Collections.singleton(groupId)).all().get();
            logger.info("Deleted consumer group: {}", groupId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted deleting consumer group " + groupId, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to delete consumer group " + groupId, e);
        }
    }

    // -----------------------------------------------------------------------
    // Cluster health
    // -----------------------------------------------------------------------

    /**
     * Returns cluster health info: broker count, broker details, controller.
     */
    public ClusterHealth getClusterHealth() {
        try {
            DescribeClusterResult cluster = admin.describeCluster();
            // 10s timeout — if the controller is wedged this must fail fast
            // so the health reporter can mark the instance UNHEALTHY and k8s
            // can restart the pod. The default AdminClient timeout (60s) is
            // far too long for a liveness probe.
            Collection<Node> nodes = cluster.nodes().get(10, TimeUnit.SECONDS);
            Node controller = cluster.controller().get(10, TimeUnit.SECONDS);

            List<BrokerInfo> brokers = new ArrayList<>();
            for (Node node : nodes) {
                brokers.add(new BrokerInfo(
                        node.id(), node.host(), node.port(),
                        node.id() == controller.id()));
            }

            return new ClusterHealth(nodes.size(), brokers, controller.id());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted getting cluster health", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to get cluster health", e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Cluster health check timed out — controller may be wedged", e);
        }
    }

    @Override
    public void close() {
        admin.close();
    }

    // -----------------------------------------------------------------------
    // Records
    // -----------------------------------------------------------------------

    public record TopicInfo(String name, int partitionCount, long retentionMs,
                            Map<Integer, Long> earliestOffsets, Map<Integer, Long> latestOffsets) {
    }

    public record PartitionLag(String topic, int partition,
                                long committedOffset, long endOffset, long lag) {
    }

    public record ConsumerGroupInfo(String groupId, String state,
                                     List<PartitionLag> partitionLags, long totalLag) {
    }

    public record BrokerInfo(int id, String host, int port, boolean isController) {
    }

    public record ClusterHealth(int brokerCount, List<BrokerInfo> brokers, int controllerId) {
    }
}
