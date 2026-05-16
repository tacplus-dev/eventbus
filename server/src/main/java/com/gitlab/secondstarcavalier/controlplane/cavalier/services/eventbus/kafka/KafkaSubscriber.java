// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps KafkaConsumer with seek/replay, speed-controlled pacing,
 * pause/resume, server-side backpressure policies, and partition-aware
 * subscription.
 *
 * Supports mid-stream resubscribe via bidi streaming — the client sends
 * a new SubscribeRequest at any time to seek, change speed, pause,
 * or switch backpressure policy without tearing down the connection.
 *
 * Speed control uses original Kafka record timestamps to pace delivery:
 *   0   = max throughput (no delay, catch-up mode)
 *   1.0 = real-time (events replayed at original rate)
 *  10.0 = 10x fast-forward
 *
 * Backpressure policies:
 *   BUFFER   = default gRPC behaviour (server buffers, risks OOM)
 *   DROP_OLD = skip to latest offset when client falls behind
 *   SAMPLE   = deliver every Nth event, drop the rest
 *
 * gRPC flow control provides additional client-side backpressure on top.
 */
public class KafkaSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSubscriber.class);
    private final String bootstrapServers;

    public KafkaSubscriber(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * A consumed Kafka record with full metadata.
     */
    public record Record(
            String topic,
            int partition,
            long offset,
            long timestamp,
            String key,
            Map<String, String> headers,
            byte[] value
    ) {}

    /**
     * Callback interface for consuming records.
     */
    public interface RecordHandler {
        /** Returns false to stop consuming. */
        boolean onRecord(Record record);
    }

    /**
     * Seek mode for subscription start position.
     */
    public enum SeekMode {
        OFFSET, TIMESTAMP, LATEST, EARLIEST
    }

    /**
     * Server-side backpressure policy.
     */
    public enum BackpressurePolicy {
        BUFFER, DROP_OLD, SAMPLE
    }

    /**
     * Immutable snapshot of a subscribe request's seek + pacing parameters.
     * The gRPC layer creates these from each incoming SubscribeRequest and
     * posts them to the consumer thread via {@link Session#resubscribe}.
     */
    public record SeekParams(
            SeekMode mode,
            long value,
            double speedMultiplier,
            String consumerGroup,
            boolean pause,
            BackpressurePolicy backpressure,
            int sampleInterval,
            List<Integer> partitions
    ) {
        public SeekParams {
            if (backpressure == null) backpressure = BackpressurePolicy.BUFFER;
            if (sampleInterval < 1) sampleInterval = 1;
            if (partitions == null) partitions = List.of();
        }
    }

    /**
     * A long-lived subscribe session tied to a single Kafka consumer.
     * The gRPC bidi stream handler holds a reference and calls
     * {@link #resubscribe} when the client sends a new SubscribeRequest.
     * The poll loop picks up the new params on the next iteration.
     */
    public class Session implements AutoCloseable {
        private final String topic;
        private final KafkaConsumer<String, byte[]> consumer;
        private final AtomicReference<SeekParams> pending = new AtomicReference<>();
        private volatile boolean closed;

        private Session(String topic, KafkaConsumer<String, byte[]> consumer) {
            this.topic = topic;
            this.consumer = consumer;
        }

        /**
         * Enqueue a seek reset. The poll loop applies it on the next iteration.
         * Thread-safe — called from the gRPC request stream thread.
         */
        public void resubscribe(SeekParams params) {
            pending.set(params);
            consumer.wakeup(); // break out of current poll() immediately
        }

        @Override
        public void close() {
            closed = true;
            consumer.wakeup();
        }

        /**
         * Runs the poll loop. Blocks until closed or interrupted.
         * Closes the underlying KafkaConsumer when the loop exits.
         */
        public void run(SeekParams initial, RecordHandler handler) {
            try {
                runLoop(initial, handler);
            } catch (Exception e) {
                logger.error("runLoop exited with exception on topic {}: {}", topic, e.toString(), e);
            } finally {
                consumer.close();
            }
        }

        private void runLoop(SeekParams initial, RecordHandler handler) {
            logger.info("runLoop starting: topic={}, mode={}, speed={}, pause={}", topic, initial.mode(), initial.speedMultiplier(), initial.pause());
            Set<TopicPartition> partitions = consumer.assignment();
            // Wait for partition assignment — the initial poll in createSession may not
            // have been long enough for group rebalancing to complete.
            if (partitions.isEmpty()) {
                for (int attempt = 0; attempt < 5 && !closed; attempt++) {
                    logger.info("Waiting for partition assignment on topic {} (attempt {})", topic, attempt + 1);
                    try {
                        consumer.poll(Duration.ofMillis(5000));
                    } catch (org.apache.kafka.common.errors.WakeupException e) {
                        if (closed) return;
                    }
                    partitions = consumer.assignment();
                    if (!partitions.isEmpty()) break;
                }
                if (partitions.isEmpty()) {
                    logger.warn("No partitions assigned for topic {} after retries", topic);
                    return;
                }
            }

            applySeek(consumer, partitions, initial);
            double speedMultiplier = initial.speedMultiplier();
            boolean paused = initial.pause();
            BackpressurePolicy backpressure = initial.backpressure();
            int sampleInterval = initial.sampleInterval();
            boolean paceByTimestamp = speedMultiplier > 0 && initial.mode() != SeekMode.LATEST;
            long prevRecordTimestamp = -1;
            long prevDeliveryNanos = System.nanoTime();
            long sampleCounter = 0;

            while (!closed && !Thread.currentThread().isInterrupted()) {
                // Check for pending resubscribe before polling.
                SeekParams newParams = pending.getAndSet(null);
                if (newParams != null) {
                    logger.info("Resubscribe: mode={}, value={}, speed={}, pause={}, backpressure={}, sample={}",
                            newParams.mode(), newParams.value(), newParams.speedMultiplier(),
                            newParams.pause(), newParams.backpressure(), newParams.sampleInterval());
                    applySeek(consumer, partitions, newParams);
                    speedMultiplier = newParams.speedMultiplier();
                    paused = newParams.pause();
                    backpressure = newParams.backpressure();
                    sampleInterval = newParams.sampleInterval();
                    paceByTimestamp = speedMultiplier > 0 && newParams.mode() != SeekMode.LATEST;
                    prevRecordTimestamp = -1;
                    prevDeliveryNanos = System.nanoTime();
                    sampleCounter = 0;
                }

                // Pause: hold position, wait for new params to unpause.
                if (paused) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    continue;
                }

                ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    continue; // resubscribe or close pending
                }

                // DROP_OLD: if we got a full batch, the client may be behind.
                // Skip to end so the next poll gets only the latest events.
                if (backpressure == BackpressurePolicy.DROP_OLD && records.count() >= 500) {
                    if (!partitions.isEmpty()) {
                        logger.info("DROP_OLD: client behind, seeking to end on topic {}", topic);
                        consumer.seekToEnd(partitions);
                        // Force a position fetch so the next poll starts from the end.
                        for (TopicPartition tp : partitions) {
                            consumer.position(tp);
                        }
                    }
                    continue;
                }

                for (ConsumerRecord<String, byte[]> record : records) {
                    // SAMPLE: only deliver every Nth record.
                    if (backpressure == BackpressurePolicy.SAMPLE && sampleInterval > 1) {
                        sampleCounter++;
                        if (sampleCounter % sampleInterval != 0) {
                            continue;
                        }
                    }

                    // Pace delivery based on original event timestamps.
                    if (paceByTimestamp && prevRecordTimestamp >= 0) {
                        long eventDeltaMs = record.timestamp() - prevRecordTimestamp;
                        if (eventDeltaMs > 0) {
                            long targetDelayMs = (long) (eventDeltaMs / speedMultiplier);
                            long elapsedMs = (System.nanoTime() - prevDeliveryNanos) / 1_000_000;
                            long sleepMs = targetDelayMs - elapsedMs;
                            if (sleepMs > 0) {
                                try {
                                    Thread.sleep(sleepMs);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                                // Check for resubscribe after waking from sleep.
                                if (pending.get() != null) break;
                            }
                        }
                    }

                    prevRecordTimestamp = record.timestamp();
                    prevDeliveryNanos = System.nanoTime();

                    // Extract headers as string map.
                    Map<String, String> headers = new HashMap<>();
                    for (Header h : record.headers()) {
                        headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
                    }

                    Record rec = new Record(
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.timestamp(),
                            record.key(),
                            headers,
                            record.value()
                    );

                    if (!handler.onRecord(rec)) {
                        return;
                    }
                }
                try {
                    consumer.commitSync();
                } catch (org.apache.kafka.clients.consumer.CommitFailedException e) {
                    if (e.getMessage() != null && e.getMessage().contains("rebalance")) {
                        logger.warn("commitSync failed during rebalance on topic {}, continuing: {}", topic, e.getMessage());
                    } else {
                        logger.error("commitSync failed (non-rebalance) on topic {}: {}", topic, e.getMessage(), e);
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Creates a subscribe session without starting it. The caller runs the
     * session on a dedicated thread via {@link Session#run} and posts
     * resubscribe requests via {@link Session#resubscribe}.
     *
     * The session closes its consumer when {@link Session#run} exits.
     */
    public Session createSession(String topic, SeekParams initial) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                initial.consumerGroup() != null && !initial.consumerGroup().isEmpty()
                        ? initial.consumerGroup() : UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Allow time for group rebalancing to complete before timing out.
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);

        if (!initial.partitions().isEmpty()) {
            // Manual partition assignment — no consumer-group rebalancing.
            List<TopicPartition> tps = initial.partitions().stream()
                    .map(p -> new TopicPartition(topic, p))
                    .toList();
            consumer.assign(tps);
        } else {
            // Group-managed subscription with rebalancing.
            consumer.subscribe(Collections.singletonList(topic));
            // Initial poll to trigger partition assignment — allow enough time
            // for the consumer group rebalance protocol to complete.
            consumer.poll(Duration.ofMillis(5000));
        }

        return new Session(topic, consumer);
    }

    private static void applySeek(KafkaConsumer<String, byte[]> consumer,
                                  Set<TopicPartition> partitions,
                                  SeekParams params) {
        switch (params.mode()) {
            case OFFSET:
                for (TopicPartition tp : partitions) {
                    consumer.seek(tp, params.value());
                }
                break;
            case TIMESTAMP:
                Map<TopicPartition, Long> timestamps = new HashMap<>();
                for (TopicPartition tp : partitions) {
                    timestamps.put(tp, params.value());
                }
                Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestamps);
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsets.entrySet()) {
                    if (entry.getValue() != null) {
                        consumer.seek(entry.getKey(), entry.getValue().offset());
                    }
                }
                break;
            case LATEST:
                consumer.seekToEnd(partitions);
                break;
            case EARLIEST:
                consumer.seekToBeginning(partitions);
                break;
        }
    }
}
