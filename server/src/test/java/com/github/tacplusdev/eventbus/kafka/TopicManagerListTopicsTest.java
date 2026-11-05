// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * listTopics() lists the names, then describes each one. A topic deleted
 * between the two steps used to fail the whole list (#24): the controller
 * accepts a delete before every broker's metadata has caught up, so a list
 * taken right after it can still name the topic, whose describe then fails
 * with UnknownTopicOrPartitionException.
 *
 * <p>The names come from a real embedded broker; the admin client in front of
 * it answers describeTopics for one name as a vanished topic, so the window
 * is hit every run rather than only on a loaded machine.
 */
class TopicManagerListTopicsTest {

    private static final String KEPT = "test.listtopics.Kept";
    private static final String GONE = "test.listtopics.Gone";
    private static final String DENIED = "test.listtopics.Denied";

    private static KafkaEmbeddedBroker broker;
    private static Admin realAdmin;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startBroker() throws Exception {
        Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed("topic-manager-list-topics-test");
        KafkaEmbeddedBroker.BrokerDefaults defaults =
                new KafkaEmbeddedBroker.BrokerDefaults(1, 1, 1);
        List<KafkaEmbeddedBroker.Peer> voters =
                List.of(new KafkaEmbeddedBroker.Peer(1, "localhost", 39793));

        broker = new KafkaEmbeddedBroker(
                1, KafkaEmbeddedBroker.Role.COMBINED, tempDir.resolve("kafka-data"),
                "localhost", 39792, 39793,
                voters, clusterId, defaults, null);
        Thread.sleep(3000);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.localAddress());
        realAdmin = Admin.create(props);

        TopicManager setup = new TopicManager(broker.localAddress(), 1, 1, 1);
        try {
            setup.ensureTopicExists(KEPT);
            setup.ensureTopicExists(GONE);
            setup.ensureTopicExists(DENIED);
        } finally {
            setup.close();
        }
    }

    @AfterAll
    static void stopBroker() {
        if (realAdmin != null) realAdmin.close();
        if (broker != null) broker.close();
    }

    @Test
    void listTopics_skipsATopicGoneByDescribeTime() {
        TopicManager tm = new TopicManager(failingDescribe(GONE,
                new UnknownTopicOrPartitionException("This server does not host this topic-partition.")), 1, 1, 1);
        // Only GONE is answered as vanished; DENIED describes normally here.
        List<String> names = tm.listTopics().stream().map(TopicManager.TopicInfo::name).toList();

        assertFalse(names.contains(GONE), "a topic gone by describe time should be left out: " + names);
        assertTrue(names.contains(KEPT), "the remaining topics should be listed: " + names);
        assertTrue(names.contains(DENIED), "the remaining topics should be listed: " + names);
    }

    @Test
    void listTopics_stillThrowsOnAnyOtherDescribeFailure() {
        TopicManager tm = new TopicManager(failingDescribe(DENIED,
                new TopicAuthorizationException("Not authorized to access topics: [" + DENIED + "]")), 1, 1, 1);

        RuntimeException e = assertThrows(RuntimeException.class, tm::listTopics);
        assertInstanceOf(ExecutionException.class, e.getCause());
        assertInstanceOf(TopicAuthorizationException.class, e.getCause().getCause());
    }

    /**
     * The real admin client, except that describing {@code topic} fails with
     * {@code error}. Closing it leaves the shared real client open.
     */
    private static Admin failingDescribe(String topic, Exception error) {
        return (Admin) Proxy.newProxyInstance(
                Admin.class.getClassLoader(), new Class<?>[]{Admin.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) return null;
                    if (method.getName().equals("describeTopics")
                            && args.length == 1
                            && args[0] instanceof Collection<?> names
                            && names.contains(topic)) {
                        KafkaFutureImpl<TopicDescription> failed = new KafkaFutureImpl<>();
                        failed.completeExceptionally(error);
                        return new FailedDescribe(Map.of(topic, failed));
                    }
                    try {
                        return method.invoke(realAdmin, args);
                    } catch (InvocationTargetException ite) {
                        throw ite.getCause();
                    }
                });
    }

    private static final class FailedDescribe extends DescribeTopicsResult {
        FailedDescribe(Map<String, KafkaFuture<TopicDescription>> futures) {
            super(null, futures);
        }
    }
}
