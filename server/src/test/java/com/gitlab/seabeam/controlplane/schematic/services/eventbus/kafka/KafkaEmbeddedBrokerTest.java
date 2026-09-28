// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaEmbeddedBrokerTest {

    @Test
    void clusterIdFromSeed_deterministic() {
        Uuid id1 = KafkaEmbeddedBroker.clusterIdFromSeed("test-cluster");
        Uuid id2 = KafkaEmbeddedBroker.clusterIdFromSeed("test-cluster");
        assertEquals(id1, id2, "same seed should produce same cluster ID");
    }

    @Test
    void clusterIdFromSeed_differentSeeds() {
        Uuid id1 = KafkaEmbeddedBroker.clusterIdFromSeed("cluster-a");
        Uuid id2 = KafkaEmbeddedBroker.clusterIdFromSeed("cluster-b");
        assertNotEquals(id1, id2, "different seeds should produce different cluster IDs");
    }

    @Test
    void peerToVoterEntry_format() {
        KafkaEmbeddedBroker.Peer peer = new KafkaEmbeddedBroker.Peer(1, "node1.example.com", 9093);
        assertEquals("1@node1.example.com:9093", peer.toVoterEntry());
    }

    @Test
    void role_processRoles() {
        assertEquals("broker,controller", KafkaEmbeddedBroker.Role.COMBINED.processRoles());
        assertEquals("broker", KafkaEmbeddedBroker.Role.BROKER.processRoles());
        assertEquals("controller", KafkaEmbeddedBroker.Role.CONTROLLER.processRoles());
    }

    @Test
    void role_isControllerBroker() {
        assertTrue(KafkaEmbeddedBroker.Role.COMBINED.isController());
        assertTrue(KafkaEmbeddedBroker.Role.COMBINED.isBroker());

        assertFalse(KafkaEmbeddedBroker.Role.BROKER.isController());
        assertTrue(KafkaEmbeddedBroker.Role.BROKER.isBroker());

        assertTrue(KafkaEmbeddedBroker.Role.CONTROLLER.isController());
        assertFalse(KafkaEmbeddedBroker.Role.CONTROLLER.isBroker());
    }

    @Test
    void brokerDefaults_effectiveReplicationFactor() {
        var defaults = new KafkaEmbeddedBroker.BrokerDefaults(6, 3, 168);

        // 5 brokers: RF stays at 3
        assertEquals(3, defaults.effectiveReplicationFactor(5));

        // 2 brokers: RF clamped to 2
        assertEquals(2, defaults.effectiveReplicationFactor(2));

        // 1 broker: RF clamped to 1
        assertEquals(1, defaults.effectiveReplicationFactor(1));

        // 0 brokers: RF clamped to 1 (defensive)
        assertEquals(1, defaults.effectiveReplicationFactor(0));
    }

    @Test
    void tlsConfig_recordAccessors() {
        var tls = new KafkaEmbeddedBroker.TlsConfig("cert-pem", "key-pem", "ca-pem");
        assertEquals("cert-pem", tls.certChainPem());
        assertEquals("key-pem", tls.privateKeyPem());
        assertEquals("ca-pem", tls.trustedCaPem());
    }
}
