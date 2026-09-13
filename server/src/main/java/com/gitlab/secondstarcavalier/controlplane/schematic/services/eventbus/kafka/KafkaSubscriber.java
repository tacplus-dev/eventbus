// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    /**
     * Maximum number of consumer.poll() attempts the partition-assignment
     * loop in {@link Session#runLoop} will make before failing the subscribe.
     * Each attempt asks for {@link #ASSIGNMENT_POLL_MS}. Total budget is
     * {@code ASSIGNMENT_MAX_ATTEMPTS * ASSIGNMENT_POLL_MS} milliseconds,
     * enforced as a wall-clock deadline by {@link #ASSIGNMENT_WATCHDOG} — the
     * attempt count alone cannot enforce it, because {@code poll(timeout)}
     * does not bound the JoinGroup/SyncGroup handshake and can block far past
     * its argument when the consumer group is stuck rebalancing.
     * Override via env var {@code EVENTBUS_SUBSCRIBER_ASSIGNMENT_ATTEMPTS}.
     */
    static final int ASSIGNMENT_MAX_ATTEMPTS =
            intFromEnv("EVENTBUS_SUBSCRIBER_ASSIGNMENT_ATTEMPTS", 12);

    /**
     * Per-attempt poll timeout (ms) while waiting for partition assignment.
     * Override via env var {@code EVENTBUS_SUBSCRIBER_ASSIGNMENT_POLL_MS}.
     */
    static final long ASSIGNMENT_POLL_MS =
            longFromEnv("EVENTBUS_SUBSCRIBER_ASSIGNMENT_POLL_MS", 5000L);

    /**
     * Hard ceiling (ms) on how long a session may go without calling poll()
     * before the group coordinator evicts it, and the value handed to
     * {@code max.poll.interval.ms}. The two must agree: every keep-alive in
     * the poll loop below is sized against this constant.
     *
     * Heartbeats come from the client's background thread, so a session that
     * stops polling still looks alive to the coordinator until this expires —
     * which is exactly the failure where the group reports Stable, the pod is
     * 1/1 Ready, and nothing is consumed.
     * Override via env var {@code EVENTBUS_SUBSCRIBER_MAX_POLL_INTERVAL_MS}.
     */
    static final long MAX_POLL_INTERVAL_MS =
            longFromEnv("EVENTBUS_SUBSCRIBER_MAX_POLL_INTERVAL_MS", 60000L);

    /**
     * Budget (ms) for work done between two poll() calls — pacing sleeps,
     * record dispatch to a slow gRPC client, everything on the poll thread.
     * Half of {@link #MAX_POLL_INTERVAL_MS}, so a session that overruns the
     * budget still has a full interval of slack to get back into poll().
     * Also the scan period of the poll-stall watchdog.
     */
    static final long PROCESSING_BUDGET_MS = MAX_POLL_INTERVAL_MS / 2;

    /**
     * {@code max.poll.records}. Set explicitly because the DROP_OLD
     * "client has fallen behind" check in the poll loop compares a batch size
     * against it; leaving it on the client default means that check silently
     * stops matching if the default ever moves.
     */
    static final int MAX_POLL_RECORDS =
            intFromEnv("EVENTBUS_SUBSCRIBER_MAX_POLL_RECORDS", 500);

    /** Poll timeout (ms) used while a subscription is client-paused. */
    private static final long PAUSED_POLL_MS = 100;

    /**
     * Arms the wall-clock deadline on each session's partition-assignment wait.
     * {@code KafkaConsumer.wakeup()} is the only thread-safe consumer method,
     * so this is a safe way to break a poll() that is blocked inside a
     * rebalance. One shared daemon thread — each session arms a single-shot
     * task and cancels it as soon as the wait ends.
     */
    private static final ScheduledExecutorService ASSIGNMENT_WATCHDOG =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "eventbus-assignment-watchdog");
                t.setDaemon(true);
                return t;
            });

    /**
     * Every session currently inside {@link Session#run}. Scanned by the
     * poll-stall watchdog below; a session adds itself when its loop starts
     * and removes itself when the loop exits.
     */
    private static final Set<Session> LIVE_SESSIONS = ConcurrentHashMap.newKeySet();

    static {
        // Liveness signal. A session that stops calling poll() takes its whole
        // consumer group down with it (the coordinator cannot complete a
        // rebalance until every member rejoins) and does it silently — no
        // exception, no unhealthy pod, just lag that stops moving. The loop
        // below is written so this never happens; this scan is what tells us
        // when it does anyway, instead of leaving it to be found by hand.
        ASSIGNMENT_WATCHDOG.scheduleAtFixedRate(KafkaSubscriber::scanForPollStalls,
                PROCESSING_BUDGET_MS, PROCESSING_BUDGET_MS, TimeUnit.MILLISECONDS);
    }

    private static void scanForPollStalls() {
        long now = System.nanoTime();
        for (Session session : LIVE_SESSIONS) {
            session.checkPollStall(now);
        }
    }

    private final String bootstrapServers;

    // Lifecycle counters — observable without a metrics framework. A future
    // Micrometer/Prometheus wiring can read these directly.
    private final AtomicLong sessionsStarted = new AtomicLong();
    private final AtomicLong sessionsAssigned = new AtomicLong();
    private final AtomicLong sessionsAssignTimedOut = new AtomicLong();
    private final AtomicLong sessionsFailed = new AtomicLong();
    private final AtomicLong pollStallsDetected = new AtomicLong();
    private final AtomicLong pollsCompleted = new AtomicLong();
    private final AtomicInteger sessionsActive = new AtomicInteger();

    public long getSessionsStarted()        { return sessionsStarted.get(); }
    public long getSessionsAssigned()       { return sessionsAssigned.get(); }
    public long getSessionsAssignTimedOut() { return sessionsAssignTimedOut.get(); }
    public long getSessionsFailed()         { return sessionsFailed.get(); }
    public long getPollStallsDetected()     { return pollStallsDetected.get(); }
    public long getPollsCompleted()         { return pollsCompleted.get(); }
    public int  getSessionsActive()         { return sessionsActive.get(); }

    /**
     * Thrown when a subscribe session cannot acquire any partition assignment
     * from Kafka within the retry budget. The gRPC layer surfaces this as
     * {@code UNAVAILABLE} so the client sees a real error instead of an empty
     * "successful" stream.
     */
    public static class PartitionAssignmentTimeoutException extends RuntimeException {
        public PartitionAssignmentTimeoutException(String message) {
            super(message);
        }
    }

    private static int intFromEnv(String name, int defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid integer for env {}={}, using default {}", name, v, defaultValue);
            return defaultValue;
        }
    }

    private static long longFromEnv(String name, long defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) return defaultValue;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid long for env {}={}, using default {}", name, v, defaultValue);
            return defaultValue;
        }
    }

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
        /** Set by the assignment watchdog so a deadline wakeup is not mistaken for a resubscribe. */
        private volatile boolean assignmentDeadlineExpired;
        /** When poll() last returned. Read by the poll-stall watchdog. */
        private volatile long lastPollNanos = System.nanoTime();
        /** Latches so one stall is logged once, not once per watchdog scan. */
        private volatile boolean pollStallReported;

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

        /** Records that poll() returned, clearing any outstanding stall report. */
        private void markPolled() {
            lastPollNanos = System.nanoTime();
            pollsCompleted.incrementAndGet();
            if (pollStallReported) {
                pollStallReported = false;
                logger.warn("Consumer poll loop recovered on topic {}", topic);
            }
        }

        /** Called by the shared watchdog; logs and counts a stalled poll loop once. */
        private void checkPollStall(long nowNanos) {
            if (pollStallReported || closed) return;
            long stalledMs = (nowNanos - lastPollNanos) / 1_000_000;
            if (stalledMs <= MAX_POLL_INTERVAL_MS) return;
            pollStallReported = true;
            pollStallsDetected.incrementAndGet();
            logger.error("Consumer poll loop STALLED on topic {}: no successful poll for {}ms "
                            + "(max.poll.interval.ms={}). This member will be evicted and every "
                            + "consumer in its group rebalanced; consumption stops until it polls again.",
                    topic, stalledMs, MAX_POLL_INTERVAL_MS);
        }

        /**
         * Tops up the client's poll timer without consuming anything, so a
         * long stretch of work inside one record batch cannot get this member
         * evicted (and with it stall every other member of the group).
         *
         * All assigned partitions are paused for the call and {@code
         * Duration.ZERO} is used deliberately: a zero timer cannot complete a
         * rebalance mid-batch and cannot return records — a partition handed
         * to us by a rebalance during this call has no completed fetch yet —
         * so there is nothing to buffer and nothing to drop. What it does do
         * is reset {@code lastPollTime} in the heartbeat thread, which is the
         * only thing standing between a slow handler and a group-wide eviction.
         */
        private void keepAlive() {
            Set<TopicPartition> assigned = consumer.assignment();
            consumer.pause(assigned);
            try {
                consumer.poll(Duration.ZERO);
                markPolled();
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // A resubscribe or close is pending. The caller re-checks both
                // right after this returns, so the wakeup is not lost.
            } finally {
                consumer.resume(assigned);
            }
        }

        /**
         * Sleeps a pacing delay in slices no longer than
         * {@link #PROCESSING_BUDGET_MS}, keeping group membership alive between
         * slices. Replaying at speed 1.0 across events minutes apart asks for a
         * sleep of minutes; doing that in one Thread.sleep() blows
         * max.poll.interval.ms and rebalances the whole group.
         *
         * Returns false if the thread was interrupted.
         */
        private boolean paceSleep(long totalMs) {
            long remainingMs = totalMs;
            while (remainingMs > 0 && !closed && pending.get() == null) {
                long sliceMs = Math.min(remainingMs, PROCESSING_BUDGET_MS);
                try {
                    Thread.sleep(sliceMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                remainingMs -= sliceMs;
                if (remainingMs > 0) {
                    keepAlive();
                }
            }
            return true;
        }

        /**
         * Runs the poll loop. Blocks until closed or interrupted.
         * Closes the underlying KafkaConsumer when the loop exits.
         */
        public void run(SeekParams initial, RecordHandler handler) {
            sessionsStarted.incrementAndGet();
            sessionsActive.incrementAndGet();
            lastPollNanos = System.nanoTime();
            LIVE_SESSIONS.add(this);
            long startedNanos = System.nanoTime();
            try {
                runLoop(initial, handler);
            } catch (PartitionAssignmentTimeoutException e) {
                sessionsAssignTimedOut.incrementAndGet();
                // Propagate so the gRPC layer can surface UNAVAILABLE to the client.
                throw e;
            } catch (Exception e) {
                sessionsFailed.incrementAndGet();
                logger.error("runLoop exited with exception on topic {}: {}", topic, e.toString(), e);
            } finally {
                LIVE_SESSIONS.remove(this);
                sessionsActive.decrementAndGet();
                long durationMs = (System.nanoTime() - startedNanos) / 1_000_000;
                logger.info("Subscribe session ended: topic={}, duration_ms={}, active_sessions={}, "
                                + "totals[started={}, assigned={}, assign_timeouts={}, failed={}, poll_stalls={}]",
                        topic, durationMs, sessionsActive.get(),
                        sessionsStarted.get(), sessionsAssigned.get(),
                        sessionsAssignTimedOut.get(), sessionsFailed.get(),
                        pollStallsDetected.get());
                consumer.close();
            }
        }

        private void runLoop(SeekParams initial, RecordHandler handler) {
            logger.info("runLoop starting: topic={}, mode={}, speed={}, pause={}", topic, initial.mode(), initial.speedMultiplier(), initial.pause());
            Set<TopicPartition> partitions = consumer.assignment();
            // Wait for partition assignment — the initial poll in createSession may not
            // have been long enough for group rebalancing to complete.
            if (partitions.isEmpty()) {
                // The attempt count cannot bound this wait on its own: poll(timeout)
                // does not bound the JoinGroup/SyncGroup handshake, so a group stuck
                // rebalancing parks the loop inside attempt 1 indefinitely. Arm a
                // wall-clock deadline that wakes the blocked poll() up.
                long budgetMs = ASSIGNMENT_MAX_ATTEMPTS * ASSIGNMENT_POLL_MS;
                long waitStartedNanos = System.nanoTime();
                ScheduledFuture<?> watchdog = ASSIGNMENT_WATCHDOG.schedule(() -> {
                    assignmentDeadlineExpired = true;
                    logger.warn("Partition assignment deadline of {}ms expired on topic {}, waking consumer",
                            budgetMs, topic);
                    consumer.wakeup();
                }, budgetMs, TimeUnit.MILLISECONDS);
                try {
                    for (int attempt = 0;
                         attempt < ASSIGNMENT_MAX_ATTEMPTS && !closed && !assignmentDeadlineExpired;
                         attempt++) {
                        logger.info("Waiting for partition assignment on topic {} (attempt {}/{}, poll {}ms)",
                                topic, attempt + 1, ASSIGNMENT_MAX_ATTEMPTS, ASSIGNMENT_POLL_MS);
                        try {
                            consumer.poll(Duration.ofMillis(ASSIGNMENT_POLL_MS));
                            markPolled();
                        } catch (org.apache.kafka.common.errors.WakeupException e) {
                            if (closed) return;
                            // A deadline wakeup falls through to the loop guard, which
                            // exits so the timeout below is thrown. Any other wakeup
                            // (a resubscribe) just costs this attempt.
                        }
                        partitions = consumer.assignment();
                        if (!partitions.isEmpty()) break;
                    }
                } finally {
                    watchdog.cancel(false);
                }
                if (partitions.isEmpty()) {
                    long waitedMs = (System.nanoTime() - waitStartedNanos) / 1_000_000;
                    throw new PartitionAssignmentTimeoutException(
                            "No partitions assigned for topic " + topic + " after "
                                    + waitedMs + "ms (budget " + budgetMs + "ms: "
                                    + ASSIGNMENT_MAX_ATTEMPTS + " attempts of "
                                    + ASSIGNMENT_POLL_MS + "ms each)");
                }
            }
            sessionsAssigned.incrementAndGet();
            logger.info("Partition assignment complete: topic={}, partitions={}", topic, partitions);

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
                    // A rebalance may have run while this session was paused,
                    // so re-read the assignment before seeking it — seeking a
                    // partition we no longer own throws.
                    partitions = consumer.assignment();
                    if (!newParams.pause()) {
                        consumer.resume(partitions);
                    }
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

                // Pause: hold position, wait for new params to unpause — but
                // KEEP POLLING. Sleeping here instead does not call poll(), so
                // after max.poll.interval.ms the coordinator evicts this member
                // and rebalances the group, and because the loop is asleep it
                // never rejoins: the pod stays 1/1 Ready, the group still lists
                // members, and consumption is silently dead until eventbus is
                // restarted by hand. Kafka's own pause() holds the position
                // with the group membership intact.
                if (paused) {
                    consumer.pause(partitions);
                    try {
                        ConsumerRecords<String, byte[]> drained =
                                consumer.poll(Duration.ofMillis(PAUSED_POLL_MS));
                        markPolled();
                        partitions = consumer.assignment();
                        if (!drained.isEmpty()) {
                            // Only reachable for a partition handed to us by a
                            // rebalance inside this poll — assignFromSubscribed()
                            // resets per-partition pause state. Dropping is safe:
                            // every resubscribe applies a seek before delivery
                            // resumes, so nothing is skipped.
                            logger.debug("Dropped {} record(s) fetched while paused on topic {}",
                                    drained.count(), topic);
                        }
                    } catch (org.apache.kafka.common.errors.WakeupException e) {
                        // resubscribe or close pending — handled at the top of the loop
                    }
                    continue;
                }

                ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                    markPolled();
                    partitions = consumer.assignment();
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    continue; // resubscribe or close pending
                }

                // DROP_OLD: if we got a full batch, the client may be behind.
                // Skip to end so the next poll gets only the latest events.
                if (backpressure == BackpressurePolicy.DROP_OLD && records.count() >= MAX_POLL_RECORDS) {
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
                    // Dispatching the records already handled in this batch may
                    // have taken a while — a slow gRPC client under BUFFER, or a
                    // pacing sleep. Anything that keeps this thread out of poll()
                    // past max.poll.interval.ms gets the member evicted and every
                    // consumer in its group rebalanced, so top the poll timer up
                    // before carrying on with the batch.
                    if (System.nanoTime() - lastPollNanos > PROCESSING_BUDGET_MS * 1_000_000L) {
                        keepAlive();
                        if (closed || pending.get() != null) break;
                    }

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
                                if (!paceSleep(sleepMs)) {
                                    return; // interrupted
                                }
                                // Check for resubscribe after waking from sleep.
                                if (closed || pending.get() != null) break;
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
                // Only commit when this poll actually delivered records. An
                // unconditional commit here fires on every empty poll too --
                // at poll(500ms) that is 2 commits/sec per subscription per
                // topic per group with ZERO traffic, which floods
                // __consumer_offsets, overloads the KRaft controller, and
                // eventually leaves a log the broker cannot replay on restart.
                // An empty poll advances nothing, so the commit carries no
                // information: skipping it is free.
                if (records.isEmpty()) {
                    continue;
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
        // Bound how long a wedged consumer can hold its partitions before the
        // coordinator evicts it. The 300000ms default meant a subscriber stuck
        // in a rebalance sat on the group for 5 minutes; 60000ms matches the
        // partition-assignment budget above and is 2x session.timeout.ms, well
        // clear of the 500ms poll cadence of the steady-state loop.
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Long.toString(MAX_POLL_INTERVAL_MS));
        // Bound how much work one poll() can hand the loop. The loop keeps its
        // group membership alive mid-batch (see Session.keepAlive), but a
        // smaller batch is also a shorter worst-case delay before the next real
        // poll. The DROP_OLD check above compares against this same constant.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(MAX_POLL_RECORDS));

        // Retry/reconnect backoff — prevents tight FindCoordinator loops from
        // overwhelming the KRaft controller when coordinator lookup fails.
        // Without these, Kafka client defaults are 100ms/50ms/1000ms, which
        // produces ~5-10 req/sec against a struggling controller and converts
        // a transient blip into a full outage (observed in drill 7 pre-check).
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000");
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "30000");

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
            //
            // This runs on the gRPC request thread, and poll(timeout) does not
            // bound the JoinGroup/SyncGroup handshake (see runLoop below), so a
            // group stuck rebalancing would park that thread indefinitely. Arm
            // the same wall-clock deadline the assignment wait uses. If it
            // fires, assignment is left to runLoop's watchdogged wait.
            ScheduledFuture<?> watchdog = ASSIGNMENT_WATCHDOG.schedule(() -> {
                logger.warn("Initial assignment poll of {}ms exceeded on topic {}, waking consumer",
                        ASSIGNMENT_POLL_MS, topic);
                consumer.wakeup();
            }, ASSIGNMENT_POLL_MS, TimeUnit.MILLISECONDS);
            try {
                consumer.poll(Duration.ofMillis(ASSIGNMENT_POLL_MS));
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // Deadline expired mid-handshake — runLoop waits for assignment.
            } finally {
                watchdog.cancel(false);
            }
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
