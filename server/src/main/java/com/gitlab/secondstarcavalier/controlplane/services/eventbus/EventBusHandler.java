// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.services.eventbus;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.secondstarcavalier.controlplane.services.eventbus.kafka.KafkaEmbeddedBroker;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.kafka.KafkaPublisher;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.kafka.KafkaSubscriber;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.kafka.TopicManager;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.generated.TypedEventBusBindings;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.services.eventbus.Eventbus;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.services.eventbus.EventbusStateServiceGrpc;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.services.eventbus.Helpers;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.libs.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.libs.host.HostOuterClass;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.libs.key.KeyOuterClass;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.libs.x509.X509;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.common.tokenrequest.Tokenrequest;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.services.kernel.MintGrpc;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.libs.servicemanager.Caller;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.libs.servicemanager.Auth;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.libs.x509.ImmutableCertChain;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.s2sm.Handler;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.s2sm.ServiceManager;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.s2sm.internal.grpc.servers.services.eventbus.Admin.AdminHandler;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.MetadataUtils;

/**
 * EventBusHandler implements the tacplus-generated Handler interface.
 *
 * Lifecycle:
 * 1. handleEventBusConfig() — validate config, start embedded Kafka, create clients
 * 2. getEventBusGrpcImpl() — returns the EventBusServiceHandler for control RPCs
 * 3. additionalServices() — registers typed Publish/Subscribe RPC bindings
 * 4. shutdown() — closes Kafka clients and broker
 */
public class EventBusHandler implements Handler {

	private static final Logger logger = LoggerFactory.getLogger(EventBusHandler.class);
	private static final String SERVICE_NAME = "eventbus";
	private static final long LEAF_CERT_LIFETIME_SECONDS = 24L * 3600;
	private static final long CERT_RENEWAL_SECONDS = 20L * 3600;

	private final ServiceManager sm;
	private final ScheduledExecutorService refreshScheduler;

	// Initialized in handleEventBusConfig
	private KafkaEmbeddedBroker broker;
	private KafkaPublisher publisher;
	private KafkaSubscriber subscriber;
	private TopicManager topicManager;
	private EventBusServiceHandlerImpl handlerImpl;

	public EventBusHandler(ServiceManager sm) {
		this.sm = sm;
		this.refreshScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "eventbus-state-refresh");
			t.setDaemon(true);
			return t;
		});
	}

	@Override
	public AdminHandler getAdminGrpcImpl() {
		return handlerImpl;
	}

	@Override
	public void handleAdminConfig(Eventbus.Config config) throws Exception {
		// Config already has defaults applied and env vars merged by ServiceManager.
		Helpers.validateConfig(config);

		// Validate node role
		Eventbus.KafkaNodeRole nodeRole = config.getNodeRole();
		if (nodeRole == Eventbus.KafkaNodeRole.KAFKA_ROLE_UNDEFINED
				|| nodeRole == Eventbus.KafkaNodeRole.UNRECOGNIZED) {
			throw new IllegalStateException(
					"FATAL: node_role not set — set PROTO_CONFIG_NODE_ROLE env var or configure in kernel");
		}
		validateTopology(config, nodeRole);

		int brokerPort = config.getBrokerPort();
		int controllerPort = config.getControllerPort();
		int nodeId = config.getNodeId();
		String clusterSeed = config.getClusterSeed();
		String envHost = System.getenv("HOSTNAME");
		String hostname = (envHost != null && !envHost.isEmpty()) ? envHost : "localhost";

		KafkaEmbeddedBroker.Role role = switch (nodeRole) {
			case KAFKA_ROLE_BROKER -> KafkaEmbeddedBroker.Role.BROKER;
			case KAFKA_ROLE_CONTROLLER -> KafkaEmbeddedBroker.Role.CONTROLLER;
			default -> KafkaEmbeddedBroker.Role.COMBINED;
		};

		String dataDir = config.getDataDir();
		int retentionHours = config.getDefaultRetentionHours();
		int partitions = config.getDefaultPartitions();
		int replicationFactor = config.getDefaultReplicationFactor();

		logger.info("Starting event-bus: role={}, nodeId={}, dataDir={}", role, nodeId, dataDir);

		// Inter-node mTLS
		ManagedChannel kernelChan = sm.conn();
		KafkaEmbeddedBroker.TlsConfig tls = obtainInterNodeTls(kernelChan, hostname);
		logger.info("Inter-node mTLS enabled with {} DNS SAN (hostname={})", SERVICE_NAME, hostname);

		// Peer discovery
		List<KafkaEmbeddedBroker.Peer> voters = discoverVotersFromState(
				nodeId, hostname, controllerPort, nodeRole,
				config.getStateRefreshIntervalSeconds());

		// Embedded Kafka
		KafkaEmbeddedBroker.BrokerDefaults defaults = new KafkaEmbeddedBroker.BrokerDefaults(partitions,
				replicationFactor, retentionHours);
		org.apache.kafka.common.Uuid clusterId = KafkaEmbeddedBroker.clusterIdFromSeed(clusterSeed);

		broker = new KafkaEmbeddedBroker(
				nodeId, role, Path.of(dataDir),
				hostname, brokerPort, controllerPort,
				voters, clusterId, defaults, tls);

		// TLS cert renewal at half-life
		refreshScheduler.scheduleAtFixedRate(() -> {
			try {
				KafkaEmbeddedBroker.TlsConfig renewed = obtainInterNodeTls(sm.conn(), hostname);
				broker.updateTls(renewed);
				logger.info("Inter-node TLS cert renewed");
			} catch (Exception e) {
				logger.error("Failed to renew inter-node TLS cert: {}", e.getMessage());
			}
		}, CERT_RENEWAL_SECONDS, CERT_RENEWAL_SECONDS, TimeUnit.SECONDS);

		// Kafka clients (broker/combined only)
		if (role.isBroker()) {
			String localBootstrap = broker.localAddress();
			topicManager = new TopicManager(
					localBootstrap, partitions,
					defaults.effectiveReplicationFactor(voters.size()),
					retentionHours);
			publisher = new KafkaPublisher(localBootstrap);
			subscriber = new KafkaSubscriber(localBootstrap);
		}

		// Create the handler impl
		handlerImpl = new EventBusServiceHandlerImpl(topicManager, publisher, subscriber);

		logger.info("Event-bus initialized: role={}, nodeId={}", role, nodeId);
	}

	@Override
	public List<ServerServiceDefinition> additionalServices() {
		if (publisher != null && subscriber != null && topicManager != null) {
			// Wrap typed RPCs with the request-id interceptor so that
			// Auth.tokenPayloadFromContext() returns the decoded JWT payload.
			// This enables PublishHandler to stamp identity claims from C2S tokens.
			ServerServiceDefinition typed = TypedEventBusBindings.bindAll(publisher, subscriber, topicManager);
			ServerServiceDefinition intercepted = ServerInterceptors.intercept(
					typed, Auth.requestIdInterceptor());
			return List.of(intercepted);
		}
		return Collections.emptyList();
	}

	@Override
	public void newTlsCert(X509.X509CertChain certChain, byte[] privateKeyDer) throws Exception {
		logger.info("Received new TLS certificate from kernel ({} certs in chain)",
				certChain.getChainCount());
	}

	@Override
	public void onEventStream(byte[] event, Handler.SubscriptionMeta meta) throws Exception {
		logger.debug("Received event stream record at offset={}", meta.offset);
	}

	@Override
	public void onTraceEvents(byte[] event, Handler.SubscriptionMeta meta) throws Exception {
		logger.debug("Received trace event at offset={}", meta.offset);
	}

	@Override
	public void shutdown() {
		logger.info("Shutting down event-bus");
		refreshScheduler.shutdownNow();
		if (publisher != null)
			publisher.close();
		if (topicManager != null)
			topicManager.close();
		if (broker != null)
			broker.close();
	}

	// -----------------------------------------------------------------------
	// EventBusServiceHandler implementation
	// -----------------------------------------------------------------------

	private static class EventBusServiceHandlerImpl implements AdminHandler {

		private final TopicManager topicManager;
		private final KafkaPublisher publisher;
		private final KafkaSubscriber subscriber;

		EventBusServiceHandlerImpl(TopicManager topicManager, KafkaPublisher publisher,
				KafkaSubscriber subscriber) {
			this.topicManager = topicManager;
			this.publisher = publisher;
			this.subscriber = subscriber;
		}

		@Override
		public Eventbus.ListTopicsResponse listTopics(Caller caller) throws ProtoException {
			List<TopicManager.TopicInfo> topics = topicManager.listTopics();
			Eventbus.ListTopicsResponse.Builder builder = Eventbus.ListTopicsResponse.newBuilder();
			for (TopicManager.TopicInfo info : topics) {
				builder.addTopics(toProto(info));
			}
			return builder.build();
		}

		@Override
		public Eventbus.TopicInfo getTopicInfo(Caller caller, Eventbus.TopicInfoRequest request) throws ProtoException {
			return toProto(topicManager.getTopicInfo(request.getTopic()));
		}

		@Override
		public void deleteTopic(Caller caller, Eventbus.DeleteTopicRequest request) throws ProtoException {
			topicManager.deleteTopic(request.getTopic());
		}

		@Override
		public Eventbus.TopicInfo alterTopicConfig(Caller caller, Eventbus.AlterTopicConfigRequest request)
				throws ProtoException {
			TopicManager.TopicInfo info = topicManager.alterTopicConfig(
					request.getTopic(), request.getRetentionMs(), request.getPartitionCount());
			return toProto(info);
		}

		@Override
		public Eventbus.ConsumerGroupInfo getConsumerGroupLag(Caller caller, Eventbus.ConsumerGroupRequest request)
				throws ProtoException {
			return toProto(topicManager.getConsumerGroupLag(request.getGroupId()));
		}

		@Override
		public Eventbus.ListConsumerGroupsResponse listConsumerGroups(Caller caller) throws ProtoException {
			List<TopicManager.ConsumerGroupInfo> groups = topicManager.listConsumerGroups();
			Eventbus.ListConsumerGroupsResponse.Builder builder = Eventbus.ListConsumerGroupsResponse.newBuilder();
			for (TopicManager.ConsumerGroupInfo info : groups) {
				builder.addGroups(toProto(info));
			}
			return builder.build();
		}

		@Override
		public void resetConsumerGroupOffsets(Caller caller, Eventbus.ResetConsumerGroupOffsetsRequest request)
				throws ProtoException {
			KafkaSubscriber.SeekMode mode;
			long value = 0;
			switch (request.getResetToCase()) {
				case OFFSET:
					mode = KafkaSubscriber.SeekMode.OFFSET;
					value = request.getOffset();
					break;
				case TIMESTAMP:
					mode = KafkaSubscriber.SeekMode.TIMESTAMP;
					var ts = request.getTimestamp();
					value = ts.getSeconds() * 1000 + ts.getNanos() / 1_000_000;
					break;
				case TO_LATEST:
					mode = KafkaSubscriber.SeekMode.LATEST;
					break;
				case TO_EARLIEST:
					mode = KafkaSubscriber.SeekMode.EARLIEST;
					break;
				default:
					throw new ProtoException(io.grpc.Status.INVALID_ARGUMENT.getCode().value(),
							"reset_to is required");
			}
			topicManager.resetConsumerGroupOffsets(
					request.getGroupId(), request.getTopic(), mode, value);
		}

		@Override
		public void deleteConsumerGroup(Caller caller, Eventbus.ConsumerGroupRequest request) throws ProtoException {
			topicManager.deleteConsumerGroup(request.getGroupId());
		}

		@Override
		public Eventbus.ClusterHealthResponse getClusterHealth(Caller caller) throws ProtoException {
			TopicManager.ClusterHealth health = topicManager.getClusterHealth();
			Eventbus.ClusterHealthResponse.Builder builder = Eventbus.ClusterHealthResponse.newBuilder()
					.setBrokerCount(health.brokerCount())
					.setControllerId(health.controllerId());
			for (TopicManager.BrokerInfo bi : health.brokers()) {
				builder.addBrokers(Eventbus.BrokerInfo.newBuilder()
						.setId(bi.id())
						.setHost(bi.host())
						.setPort(bi.port())
						.setIsController(bi.isController())
						.build());
			}
			return builder.build();
		}

		private static Eventbus.TopicInfo toProto(TopicManager.TopicInfo info) {
			Eventbus.TopicInfo.Builder b = Eventbus.TopicInfo.newBuilder()
					.setName(info.name())
					.setPartitionCount(info.partitionCount())
					.setRetentionMs(info.retentionMs());
			info.earliestOffsets().forEach(b::putEarliestOffsets);
			info.latestOffsets().forEach(b::putLatestOffsets);
			return b.build();
		}

		private static Eventbus.ConsumerGroupInfo toProto(TopicManager.ConsumerGroupInfo info) {
			Eventbus.ConsumerGroupInfo.Builder b = Eventbus.ConsumerGroupInfo.newBuilder()
					.setGroupId(info.groupId())
					.setState(info.state())
					.setTotalLag(info.totalLag());
			for (TopicManager.PartitionLag pl : info.partitionLags()) {
				b.addPartitions(Eventbus.PartitionLag.newBuilder()
						.setTopic(pl.topic())
						.setPartition(pl.partition())
						.setCommittedOffset(pl.committedOffset())
						.setEndOffset(pl.endOffset())
						.setLag(pl.lag())
						.build());
			}
			return b.build();
		}
	}

	// -----------------------------------------------------------------------
	// Inter-node mTLS
	// -----------------------------------------------------------------------

	private KafkaEmbeddedBroker.TlsConfig obtainInterNodeTls(ManagedChannel channel, String hostname) throws Exception {
		// Kafka's SSL implementation does not support Ed25519 keys — use EC P-384.
		KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
		kpg.initialize(new java.security.spec.ECGenParameterSpec("secp384r1"));
		KeyPair keyPair = kpg.generateKeyPair();

		byte[] pubKeyBytes = keyPair.getPublic().getEncoded();
		String pubKeyBase64 = Base64.getUrlEncoder().withoutPadding().encodeToString(pubKeyBytes);
		String keyId = UUID.randomUUID().toString();

		long now = Instant.now().getEpochSecond();
		long exp = now + LEAF_CERT_LIFETIME_SECONDS;

		X509.X509CertRequest.Builder certReq = X509.X509CertRequest.newBuilder()
						.setKey(KeyOuterClass.PublicKey.newBuilder()
								.setId(keyId)
								.setBase64(pubKeyBase64)
								.build())
						.setExp(exp)
						.setIssuer(SERVICE_NAME)
						.setKeyUsage(X509.X509KeyUsage.X509_KEY_USAGE_DIGITAL_SIGNATURE)
						.addExtKeyUsage(X509.X509ExtKeyUsage.X509_EXT_KEY_USAGE_SERVER_AUTH)
						.addExtKeyUsage(X509.X509ExtKeyUsage.X509_EXT_KEY_USAGE_CLIENT_AUTH)
						.addDnsNames(SERVICE_NAME)
						.addDnsNames("*." + SERVICE_NAME)
						.addDnsNames("localhost");
		if (!hostname.equals(SERVICE_NAME) && !hostname.equals("localhost")) {
			certReq.addDnsNames(hostname);
		}

		Tokenrequest.X509MintRequest request = Tokenrequest.X509MintRequest.newBuilder()
				.setAud(SERVICE_NAME)
				.setCertRequest(certReq.build())
				.build();

		String token = sm.getToken();
		Metadata headers = new Metadata();
		headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
				"Bearer " + token);
		MintGrpc.MintBlockingStub stub = MintGrpc.newBlockingStub(channel)
				.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

		X509.X509CertChain certChain = stub.x509Mint(request);

		ImmutableCertChain chain = ImmutableCertChain.from(certChain, keyPair.getPrivate());
		return new KafkaEmbeddedBroker.TlsConfig(
				chain.certChainPem(),
				ImmutableCertChain.privateKeyPem(keyPair.getPrivate()),
				chain.caCertsPem());
	}

	// -----------------------------------------------------------------------
	// Peer discovery
	// -----------------------------------------------------------------------

	private List<KafkaEmbeddedBroker.Peer> discoverVotersFromState(
			int nodeId, String hostname, int controllerPort,
			Eventbus.KafkaNodeRole nodeRole, int refreshIntervalSeconds) {

		ManagedChannel channel = sm.conn();
		if (channel == null) {
			logger.info("No kernel channel — starting as single-node cluster");
			return List.of(new KafkaEmbeddedBroker.Peer(nodeId, hostname, controllerPort));
		}

		var stateStub = EventbusStateServiceGrpc.newBlockingStub(channel);

		Eventbus.EventbusNode self = Eventbus.EventbusNode.newBuilder()
				.setNodeId(nodeId)
				.setRole(nodeRole)
				.setEndpoint(HostOuterClass.Host.newBuilder()
						.setHost(hostname)
						.setPort(controllerPort)
						.build())
				.build();

		Eventbus.NodesPutRequest putRequest = Eventbus.NodesPutRequest.newBuilder()
				.setKey(String.valueOf(nodeId))
				.setValue(self)
				.build();

		try {
			stateStub.setNodes(putRequest);
			logger.info("Registered node in EventbusState: nodeId={}, role={}, endpoint={}:{}",
					nodeId, nodeRole, hostname, controllerPort);
		} catch (Exception e) {
			logger.error("Failed to register node in EventbusState: {}", e.getMessage());
		}

		// Schedule refresh
		refreshScheduler.scheduleAtFixedRate(() -> {
			try {
				var refreshStub = EventbusStateServiceGrpc.newBlockingStub(sm.conn());
				refreshStub.setNodes(putRequest);
			} catch (Exception e) {
				logger.warn("Failed to refresh EventbusState registration: {}", e.getMessage());
			}
		}, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);

		// Discover peers
		List<KafkaEmbeddedBroker.Peer> voters = new ArrayList<>();
		voters.add(new KafkaEmbeddedBroker.Peer(nodeId, hostname, controllerPort));

		try {
			var response = stateStub.getAllNodes(
					Eventbus.NodesGetAllRequest.getDefaultInstance());
			for (var entry : response.getEntriesMap().entrySet()) {
				Eventbus.EventbusNode peer = entry.getValue();
				if (peer.getNodeId() != nodeId && peer.hasEndpoint()) {
					voters.add(new KafkaEmbeddedBroker.Peer(
							peer.getNodeId(),
							peer.getEndpoint().getHost(),
							peer.getEndpoint().getPort()));
				}
			}
			logger.info("Discovered {} total voters (including self) from EventbusState", voters.size());
		} catch (Exception e) {
			logger.warn("Failed to discover peers from EventbusState — starting with self only: {}",
					e.getMessage());
		}

		return voters;
	}

	// -----------------------------------------------------------------------
	// Helpers
	// -----------------------------------------------------------------------

	private static void validateTopology(Eventbus.Config config, Eventbus.KafkaNodeRole role) {
		int controllerCount = config.getControllerCount();
		int brokerCount = config.getBrokerCount();
		switch (role) {
			case KAFKA_ROLE_CONTROLLER -> {
				if (controllerCount < 1) {
					throw new IllegalStateException(
							"FATAL: node is controller but config has controller_count=" + controllerCount);
				}
			}
			case KAFKA_ROLE_BROKER -> {
				if (brokerCount < 1) {
					throw new IllegalStateException(
							"FATAL: node is broker but config has broker_count=" + brokerCount);
				}
			}
			default -> {
			}
		}
		logger.info("Config validated: role={}, controller_count={}, broker_count={}",
				role, controllerCount, brokerCount);
	}
}
