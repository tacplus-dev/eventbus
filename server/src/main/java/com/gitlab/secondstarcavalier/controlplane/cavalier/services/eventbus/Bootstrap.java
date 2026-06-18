// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaEmbeddedBroker;

import java.util.ArrayList;
import java.util.List;

final class Bootstrap {
    private Bootstrap() {}

    static List<KafkaEmbeddedBroker.Peer> parseVoterString(String voters) {
        if (voters == null || voters.isBlank()) {
            return List.of();
        }

        List<KafkaEmbeddedBroker.Peer> parsed = new ArrayList<>();
        for (String rawEntry : voters.split(",")) {
            String entry = rawEntry.trim();
            if (entry.isEmpty()) {
                continue;
            }

            int at = entry.indexOf('@');
            int colon = entry.lastIndexOf(':');
            if (at <= 0 || colon <= at + 1 || colon == entry.length() - 1) {
                throw new IllegalArgumentException("invalid voter entry: " + entry);
            }

            int nodeId = Integer.parseInt(entry.substring(0, at));
            String host = entry.substring(at + 1, colon);
            int controllerPort = Integer.parseInt(entry.substring(colon + 1));
            parsed.add(new KafkaEmbeddedBroker.Peer(nodeId, host, controllerPort));
        }
        return List.copyOf(parsed);
    }
}
