// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.grpc;

import com.github.tacplusdev.eventbus.kafka.KafkaEmbeddedBroker;
import com.github.tacplusdev.eventbus.kafka.KafkaPublisher;
import com.github.tacplusdev.eventbus.kafka.KafkaSubscriber;
import com.github.tacplusdev.eventbus.kafka.TopicManager;
import com.github.tacplusdev.eventbus.model.chassis.key.ImmutableKey;
import com.github.tacplusdev.eventbus.model.chassis.key.KeyHelpers;
import com.github.tacplusdev.eventbus.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.eventbus.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.eventbus.model.types.services.eventbus.Eventbus;
import com.github.tacplusdev.eventbus.model.types.services.letsencrypt.Letsencrypt;

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Trace propagation on publish (tacplus-dev/looper#51): the inbound field-200
 * {@code trace_id} of each published message arrives as the {@code trace_id}
 * header on the Kafka record that publish causes, and on no other record.
 *
 * <p>The headers used to be one map per stream, and {@code trace_id} was put
 * into it and never removed, so on a stream that sent a traced message and then
 * an untraced one, the untraced record went out carrying the earlier trace_id
 * and every subscriber attributed it to a trace that did not cause it.
 *
 * <p>Real embedded Kafka and the real {@link PublishHandler}, wired with the
 * {@code getTraceId} extractor the generator emits for every non-trace topic;
 * the records are read back off the broker, so the assertions are on what
 * landed in the log.
 */
class PublishTraceIdTest {

    private static KafkaEmbeddedBroker broker;
    private static String bootstrapAddress;
    private static TopicManager topicManager;
    private static KafkaPublisher publisher;
    private static ImmutableKey signingKey;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("publish-trace-id-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39593));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39592, 39593,
                voters, clusterId, defaults, null);

        bootstrapAddress = broker.localAddress();
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
     * One stream, three messages: trace_id "t1", then "t2", then none. Each
     * record carries its own message's trace_id, the untraced one carries no
     * trace_id header at all, and the trace dispatcher saw t1 and t2 only.
     */
    @Test
    void eachRecordCarriesItsOwnMessagesTraceId_andAnUntracedOneNone() throws Exception {
        String topic = "test.publishtraceid.Cert";
        topicManager.ensureTopicExists(topic);

        List<String> dispatched = Collections.synchronizedList(new ArrayList<>());
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
                    null, Letsencrypt.LetsEncryptCert::getTraceId, dispatched::add);
            handler.onNext(cert("first", "t1"));
            handler.onNext(cert("second", "t2"));
            handler.onNext(cert("third", ""));
            handler.onCompleted();
        });
        assertTrue(done.await(30, TimeUnit.SECONDS), "publish stream did not terminate");
        assertNull(error.get(), "publish should have succeeded: " + error.get());

        List<KafkaSubscriber.Record> records = readN(topic, 3);
        assertEquals(3, records.size(), "all three publishes should have reached the log");

        assertEquals("first", certSetId(records.get(0)));
        assertEquals("t1", records.get(0).headers().get("trace_id"),
                "the first record must carry its message's trace_id");
        assertEquals("second", certSetId(records.get(1)));
        assertEquals("t2", records.get(1).headers().get("trace_id"),
                "the second record must carry its message's trace_id");
        assertEquals("third", certSetId(records.get(2)));
        assertNull(records.get(2).headers().get("trace_id"),
                "an untraced message must not inherit an earlier message's trace_id");
        assertEquals("acme", records.get(2).headers().get("caller.namespace"),
                "the caller claims must still ride on every record");

        assertEquals(List.of("t1", "t2"), dispatched,
                "the trace dispatcher must see each non-empty trace_id, and nothing for the untraced message");
    }

    private static Letsencrypt.LetsEncryptCert cert(String certSetId, String traceId) {
        return Letsencrypt.LetsEncryptCert.newBuilder()
                .setCertSetId(certSetId)
                .setTraceId(traceId)
                .build();
    }

    private static String certSetId(KafkaSubscriber.Record record) throws Exception {
        return Letsencrypt.LetsEncryptCert.parseFrom(record.value()).getCertSetId();
    }

    /** The first n records on a topic, in log order; fewer if they do not arrive within 10s. */
    private static List<KafkaSubscriber.Record> readN(String topic, int n) throws Exception {
        KafkaSubscriber subscriber = new KafkaSubscriber(bootstrapAddress);
        KafkaSubscriber.SeekParams params = new KafkaSubscriber.SeekParams(
                KafkaSubscriber.SeekMode.EARLIEST, 0, 0, null,
                false, KafkaSubscriber.BackpressurePolicy.BUFFER, 1, List.of());
        KafkaSubscriber.Session session = subscriber.createSession(topic, params);
        try {
            List<KafkaSubscriber.Record> received = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(n);
            Thread.startVirtualThread(() -> session.run(params, record -> {
                received.add(record);
                latch.countDown();
                return received.size() < n;
            }));
            latch.await(10, TimeUnit.SECONDS);
            return new ArrayList<>(received);
        } finally {
            session.close();
        }
    }
}
