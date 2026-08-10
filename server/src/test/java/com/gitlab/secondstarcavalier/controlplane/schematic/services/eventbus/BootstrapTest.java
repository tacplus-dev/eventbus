// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka.KafkaEmbeddedBroker;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BootstrapTest {

    @Test
    void parseVoterString_singleVoter() {
        List<KafkaEmbeddedBroker.Peer> voters = Bootstrap.parseVoterString("1@node1:9093");
        assertEquals(1, voters.size());
        assertEquals(1, voters.get(0).nodeId());
        assertEquals("node1", voters.get(0).host());
        assertEquals(9093, voters.get(0).controllerPort());
    }

    @Test
    void parseVoterString_multipleVoters() {
        List<KafkaEmbeddedBroker.Peer> voters =
                Bootstrap.parseVoterString("1@host1:9093,2@host2:9093,3@host3:9093");
        assertEquals(3, voters.size());
        assertEquals(2, voters.get(1).nodeId());
        assertEquals("host2", voters.get(1).host());
    }

    @Test
    void parseVoterString_whitespace() {
        List<KafkaEmbeddedBroker.Peer> voters =
                Bootstrap.parseVoterString(" 1@host1:9093 , 2@host2:9093 ");
        assertEquals(2, voters.size());
        assertEquals("host1", voters.get(0).host());
        assertEquals("host2", voters.get(1).host());
    }
}
