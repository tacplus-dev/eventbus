// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;

/**
 * Manages an embedded KRaft-mode Kafka broker/controller.
 *
 * KRaft eliminates ZooKeeper — metadata is managed by a Raft quorum
 * of controller nodes. This class starts an in-process Kafka server
 * configured from the eventbus Config proto.
 *
 * Listener architecture:
 *   LOCAL      — plaintext, bound to localhost, used by in-process
 *                publisher/subscriber/admin. Never exposed externally.
 *   BROKER     — mTLS (when TLS configured), used for inter-broker
 *                replication traffic between eventbus instances.
 *   CONTROLLER — mTLS (when TLS configured), used for KRaft consensus
 *                quorum traffic between controller nodes.
 *
 * Peer discovery: the quorum voter list is built at startup from
 * EventbusState (stored in kernel). Each node registers itself on
 * startup and peers discover each other via getAllNodes.
 */
public class KafkaEmbeddedBroker implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEmbeddedBroker.class);

    /**
     * Kafka node role — maps directly to KafkaNodeRole proto enum values.
     */
    public enum Role {
        COMBINED("broker,controller"),
        BROKER("broker"),
        CONTROLLER("controller");

        private final String processRoles;

        Role(String processRoles) {
            this.processRoles = processRoles;
        }

        public String processRoles() {
            return processRoles;
        }

        public boolean isController() {
            return this == COMBINED || this == CONTROLLER;
        }

        public boolean isBroker() {
            return this == COMBINED || this == BROKER;
        }
    }

    /**
     * Peer info for building the quorum voter list.
     */
    public record Peer(int nodeId, String host, int controllerPort) {
        public String toVoterEntry() {
            return nodeId + "@" + host + ":" + controllerPort;
        }
    }

    /**
     * TLS configuration for inter-node and external auth.
     * When null, all listeners use PLAINTEXT.
     */
    public record TlsConfig(
            String certChainPem,   // PEM-encoded certificate chain
            String privateKeyPem,  // PEM-encoded private key
            String trustedCaPem    // PEM-encoded CA certificate(s) for trust
    ) {}

    /**
     * Broker defaults from the eventbus Config proto.
     */
    public record BrokerDefaults(
            int partitions,
            int replicationFactor,
            int retentionHours
    ) {
        /**
         * Clamp replication factor to not exceed the number of brokers.
         */
        public int effectiveReplicationFactor(int brokerCount) {
            return Math.min(replicationFactor, Math.max(1, brokerCount));
        }
    }

    private final KafkaRaftServer server;
    private final String localAddress;  // localhost:localPort — for in-process clients
    private final String brokerAddress; // host:brokerPort — for inter-broker
    private final Role role;
    private final int nodeId;

    private static final int LOCAL_PORT_OFFSET = 10000; // localPort = brokerPort + offset

    /**
     * Creates and starts an embedded Kafka broker in KRaft mode.
     *
     * @param nodeId         unique node ID
     * @param role           COMBINED, BROKER, or CONTROLLER
     * @param dataDir        persistent storage directory
     * @param host           advertised hostname for inter-node traffic
     * @param brokerPort     port for inter-broker replication
     * @param controllerPort port for KRaft controller quorum traffic
     * @param voters         all controller/combined nodes (including self)
     * @param clusterId      shared cluster UUID
     * @param defaults       partition/replication/retention defaults from Config
     * @param tls            TLS config for inter-node auth, or null for plaintext
     */
    public KafkaEmbeddedBroker(int nodeId, Role role, Path dataDir,
                                String host, int brokerPort, int controllerPort,
                                List<Peer> voters, Uuid clusterId,
                                BrokerDefaults defaults, TlsConfig tls) throws IOException {
        this.role = role;
        this.nodeId = nodeId;
        Files.createDirectories(dataDir);

        int localPort = brokerPort + LOCAL_PORT_OFFSET;
        int brokerCount = (int) voters.stream()
                .filter(p -> true) // all peers are potential brokers in combined mode
                .count();
        if (brokerCount < 1) brokerCount = 1;

        String voterString = buildVoterString(voters);
        Properties props = buildProperties(nodeId, role, dataDir, host,
                brokerPort, controllerPort, localPort,
                voterString, defaults, brokerCount, tls);

        formatStorageIfNeeded(dataDir, clusterId, nodeId);

        logger.info("Starting embedded Kafka: nodeId={}, role={}, host={}:{}/{}, local={}, voters={}, clusterId={}",
                nodeId, role, host, brokerPort, controllerPort, localPort, voterString, clusterId);

        KafkaConfig config = KafkaConfig.fromProps(props, false);
        this.server = new KafkaRaftServer(config, Time.SYSTEM);
        this.server.startup();

        this.localAddress = "localhost:" + localPort;
        this.brokerAddress = host + ":" + brokerPort;
        logger.info("Embedded Kafka started: local={}, broker={}", localAddress, brokerAddress);
    }

    /**
     * Returns the local plaintext bootstrap address for in-process Kafka clients
     * (publisher, subscriber, admin). Only valid for broker/combined roles.
     */
    public String localAddress() {
        if (!role.isBroker()) {
            throw new IllegalStateException(
                    "Controller-only nodes have no broker listener. " +
                    "Cannot create Kafka clients on a controller-only node.");
        }
        return localAddress;
    }

    /**
     * Returns the external broker address for inter-node traffic.
     */
    public String brokerAddress() {
        return brokerAddress;
    }

    /**
     * Returns the role of this node.
     */
    public Role role() {
        return role;
    }

    /**
     * Dynamically updates the TLS certificate and key on a running broker.
     * Uses Kafka AdminClient to push new SSL config without restart.
     */
    public void updateTls(TlsConfig tls) {
        if (tls == null) return;

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, localAddress);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            ConfigResource brokerResource = new ConfigResource(
                    ConfigResource.Type.BROKER, String.valueOf(nodeId));

            Collection<AlterConfigOp> ops = List.of(
                    new AlterConfigOp(
                            new org.apache.kafka.clients.admin.ConfigEntry(
                                    "ssl.keystore.certificate.chain", tls.certChainPem()),
                            AlterConfigOp.OpType.SET),
                    new AlterConfigOp(
                            new org.apache.kafka.clients.admin.ConfigEntry(
                                    "ssl.keystore.key", tls.privateKeyPem()),
                            AlterConfigOp.OpType.SET));

            admin.incrementalAlterConfigs(Map.of(brokerResource, ops)).all().get();
            logger.info("TLS certificate updated dynamically on broker nodeId={}", nodeId);
        } catch (Exception e) {
            logger.error("Failed to update TLS certificate dynamically: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        logger.info("Shutting down embedded Kafka");
        server.shutdown();
        server.awaitShutdown();
        logger.info("Embedded Kafka shut down");
    }

    private static Properties buildProperties(int nodeId, Role role, Path dataDir,
                                               String host, int brokerPort,
                                               int controllerPort, int localPort,
                                               String voters, BrokerDefaults defaults,
                                               int brokerCount, TlsConfig tls) {
        Properties props = new Properties();

        // KRaft identity
        props.put("process.roles", role.processRoles());
        props.put("node.id", String.valueOf(nodeId));
        props.put("controller.quorum.voters", voters);

        boolean useTls = tls != null;
        String brokerProtocol = useTls ? "SSL" : "PLAINTEXT";

        // Listeners — LOCAL is always plaintext (same JVM)
        StringBuilder listeners = new StringBuilder();
        if (role.isBroker()) {
            listeners.append("LOCAL://localhost:").append(localPort);
            listeners.append(",BROKER://").append(host).append(":").append(brokerPort);
        }
        if (role.isController()) {
            if (listeners.length() > 0) listeners.append(",");
            listeners.append("CONTROLLER://").append(host).append(":").append(controllerPort);
        }
        props.put("listeners", listeners.toString());

        // Advertised listeners (broker only — LOCAL + BROKER)
        if (role.isBroker()) {
            props.put("advertised.listeners",
                    "LOCAL://localhost:" + localPort + ",BROKER://" + host + ":" + brokerPort);
            props.put("inter.broker.listener.name", "BROKER");
        }

        // Controller listener
        props.put("controller.listener.names", "CONTROLLER");

        // Listener security protocol map
        props.put("listener.security.protocol.map",
                "LOCAL:PLAINTEXT,BROKER:" + brokerProtocol + ",CONTROLLER:" + brokerProtocol);

        // TLS configuration for BROKER and CONTROLLER listeners
        if (useTls) {
            // PEM-based keystore (Kafka 3.x supports ssl.keystore.type=PEM)
            props.put("ssl.keystore.type", "PEM");
            props.put("ssl.keystore.certificate.chain", tls.certChainPem());
            props.put("ssl.keystore.key", tls.privateKeyPem());

            // Truststore — CA cert(s) for verifying peers
            props.put("ssl.truststore.type", "PEM");
            props.put("ssl.truststore.certificates", tls.trustedCaPem());

            // Require client certs (mTLS)
            props.put("ssl.client.auth", "required");
        }

        // Storage
        props.put("log.dirs", dataDir.toAbsolutePath().toString());

        // Defaults from Config — clamped to actual broker count
        int effectiveRF = defaults.effectiveReplicationFactor(brokerCount);
        props.put("num.partitions", String.valueOf(defaults.partitions()));
        props.put("default.replication.factor", String.valueOf(effectiveRF));

        // Internal topics — use min(3, brokerCount) for resilience
        int internalRF = Math.min(3, Math.max(1, brokerCount));
        int internalISR = Math.min(2, internalRF);
        props.put("offsets.topic.replication.factor", String.valueOf(internalRF));
        props.put("transaction.state.log.replication.factor", String.valueOf(internalRF));
        props.put("transaction.state.log.min.isr", String.valueOf(internalISR));
        props.put("min.insync.replicas", String.valueOf(Math.min(2, effectiveRF)));

        // Log retention
        props.put("log.retention.hours", String.valueOf(defaults.retentionHours()));
        props.put("log.segment.bytes", String.valueOf(256 * 1024 * 1024)); // 256MB

        // Performance
        props.put("num.io.threads", "4");
        props.put("num.network.threads", "2");
        props.put("compression.type", "lz4");

        // --- Consumer group churn protection ---
        // Delay initial rebalance to batch consumer joins that arrive within a
        // startup window. Default is 3s; 10s gives cold-starting consumers
        // (stratum, graphdb, timedb) time to join before the first rebalance
        // fires, reducing rebalance count from N to 1.
        props.put("group.initial.rebalance.delay.ms", "10000");

        // --- KRaft metadata log bounds ---
        // Force a metadata snapshot every 5MB of new records. Default is ~100MB
        // (or no limit in some builds), which allows the __cluster_metadata log
        // to grow without bound during consumer churn (each rebalance writes
        // group coordinator records). 5MB caps the log so the controller can
        // always replay it on restart without wedging.
        props.put("metadata.log.max.record.bytes.between.snapshots", String.valueOf(5 * 1024 * 1024));
        // Snapshot at least every 60s even if the byte threshold hasn't been
        // reached — ensures the log gets compacted during quiet periods.
        props.put("metadata.max.idle.interval.ms", "60000");

        // Reduce __consumer_offsets partitions from the default 50 to 5. Each
        // partition is a metadata log entry; fewer partitions means fewer
        // metadata records during offset topic auto-creation and rebalance.
        // With a single-instance cluster, 50 partitions is pure waste.
        props.put("offsets.topic.num.partitions", "5");

        return props;
    }

    private static String buildVoterString(List<Peer> voters) {
        return String.join(",", voters.stream()
                .map(Peer::toVoterEntry)
                .toList());
    }

    private static void formatStorageIfNeeded(Path dataDir, Uuid clusterId, int nodeId) throws IOException {
        Path metaPropertiesPath = dataDir.resolve("meta.properties");
        if (Files.exists(metaPropertiesPath)) {
            logger.info("Storage already formatted at {}", dataDir);
            return;
        }

        logger.info("Formatting KRaft storage: dir={}, clusterId={}, nodeId={}",
                dataDir, clusterId, nodeId);

        MetaProperties metaProperties = new MetaProperties.Builder()
                .setVersion(MetaPropertiesVersion.V1)
                .setClusterId(clusterId.toString())
                .setNodeId(nodeId)
                .build();

        // The data dir itself must exist; nothing else is pre-created here.
        //
        // This used to also mkdir __cluster_metadata-0 "for KRaft". Kafka's own
        // StorageTool.format does not do that, and the QuorumController creates
        // the metadata log itself on first start. Leaving an EMPTY directory by
        // that name behind is actively harmful: LogManager scans the log dir at
        // startup and reads every subdirectory as a <topic>-<partition> log, so
        // it parses ours as topic __cluster_metadata partition 0, finds no
        // segments and no partition metadata, and kills the broker with
        // "Encountered fatal fault: Error starting LogManager".
        Files.createDirectories(dataDir);

        metaProperties.toProperties().store(
                Files.newOutputStream(metaPropertiesPath),
                "Generated by event-bus KafkaEmbeddedBroker");

        logger.info("KRaft storage formatted");
    }

    /**
     * Generates a deterministic cluster UUID from a seed string.
     * All nodes in the same cluster must use the same cluster ID.
     * Use the kernel cluster name as the seed for consistency.
     */
    public static Uuid clusterIdFromSeed(String seed) {
        java.util.UUID uuid = java.util.UUID.nameUUIDFromBytes(seed.getBytes());
        return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
}
