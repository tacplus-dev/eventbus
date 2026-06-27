// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaEmbeddedBroker;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaPublisher;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaSubscriber;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.TopicManager;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.generated.TopicRegistry;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.generated.TypedEventBusBindings;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.grpc.WatchTraceHandler;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.EventbusStateServiceGrpc;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Helpers;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.host.HostOuterClass;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.x509.X509;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.MetaData;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.collection.CollectionRuntime;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Auth;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.x509.ImmutableCertChain;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.Handler;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.ServiceManager;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.AdminGrpc;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.EventbusConfigStateServiceGrpc;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.trace.Trace;
import com.google.protobuf.DynamicMessage;

import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Metadata;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.grpc.PublishHandler;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.grpc.SubscribeHandler;

/**
 * EventBusHandler implements the model-generated Handler interface.
 *
 * Lifecycle:
 * 1. newTlsCert() — on first delivery, fetches eventbus config and starts Kafka
 * 2. additionalServices() — registers Admin, TraceSession, and typed Pub/Sub RPCs
 * 3. shutdown() — closes Kafka clients and broker
 */
public class EventBusHandler implements Handler {

	private static final Logger logger = LoggerFactory.getLogger(EventBusHandler.class);
	private static final String SERVICE_NAME = MetaData.PROJECT_NAME;
	private static final long[] INIT_RETRY_DELAYS_MS = {2000, 4000, 8000, 16000, 30000};

	private final ServiceManager sm;
	private final ScheduledExecutorService refreshScheduler;
	private volatile boolean initialized = false;
	private volatile boolean initStarted = false;

	// Initialized on first newTlsCert() delivery.
	// Volatile so additionalServices() (called before newTlsCert) can
	// safely observe the values set later by initialize().
	private KafkaEmbeddedBroker broker;
	private volatile KafkaPublisher publisher;
	private volatile KafkaSubscriber subscriber;
	private volatile TopicManager topicManager;
	private volatile EventBusServiceHandlerImpl handlerImpl;
	private WatchTraceHandler watchTraceHandler;
	private KafkaSubscriber.Session traceSession;
	private volatile Thread traceDispatchThread;

	public EventBusHandler(ServiceManager sm) {
		this.sm = sm;
		this.watchTraceHandler = new WatchTraceHandler();
		this.refreshScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "eventbus-state-refresh");
			t.setDaemon(true);
			return t;
		});
	}

	/**
	 * Fetch the eventbus Config from kernel and initialize Kafka.
	 * Called once on first TLS cert delivery.
	 */
	private void initialize() throws Exception {
		// Fetch eventbus-specific config from kernel's ConfigHolder.
		String token = sm.getToken();
		Metadata authHeaders = new Metadata();
		authHeaders.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
		var configStub = EventbusConfigStateServiceGrpc.newBlockingStub(sm.conn())
				.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(authHeaders));
		Eventbus.Config config = configStub.getConfig(Trace.TraceRequest.getDefaultInstance());
		// Apply defaults and merge env vars (mirrors what the generated ServiceManager does).
		config = Helpers.configDefaults(config).build();
		Eventbus.Config envCfg = Helpers.configFromEnvVar(System.getenv()).build();
		config = Helpers.mergeConfig(envCfg, config).build();
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

		// Inter-node mTLS: read the auto-TLS cert delivered by kernel's
		// TlsCertService.ManageCert bidi stream (managed by the tacplus SM).
		// The SM blocks in create() until the first cert lands, so by the time
		// initialize() runs the cert is available via sm.getTlsCertChain().
		// Renewals arrive via handler.newTlsCert() — see updateInterNodeTls below.
		KafkaEmbeddedBroker.TlsConfig tls = buildInterNodeTls(sm.getTlsCertChain(), sm.getTlsPrivateKey());
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

		// Cert renewal handled by the SM's auto-TLS bidi stream — the SM
		// invokes handler.newTlsCert(...) (see below) whenever kernel delivers
		// a renewed certificate, and that callback updates the Kafka broker.
		// No local scheduler needed.

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

		// Subscribe to the trace topic to dispatch events to WatchTrace streams
		if (subscriber != null) {
			String traceTopic = TopicRegistry.TOPIC_TRACE_EVENT_BUS_TRACE_EVENTS;
			KafkaSubscriber.SeekParams seekParams = new KafkaSubscriber.SeekParams(
					KafkaSubscriber.SeekMode.LATEST, 0, 0, "eventbus-trace-dispatch",
					false, KafkaSubscriber.BackpressurePolicy.BUFFER, 1, List.of());
			traceSession = subscriber.createSession(traceTopic, seekParams);
			traceDispatchThread = new Thread(() -> traceSession.run(seekParams, record -> {
				try {
					Trace.TraceEvent event = Trace.TraceEvent.parseFrom(record.value());
					watchTraceHandler.dispatch(event);
				} catch (Exception e) {
					logger.warn("Failed to parse trace event at offset={}: {}", record.offset(), e.getMessage());
				}
				return true;
			}), "eventbus-trace-dispatch");
			traceDispatchThread.setDaemon(true);
			traceDispatchThread.start();
			logger.info("Trace dispatch consumer started on topic {}", traceTopic);
		}

		logger.info("Event-bus initialized: role={}, nodeId={}", role, nodeId);
	}

	@Override
	public List<ServerServiceDefinition> additionalServices() {
		List<ServerServiceDefinition> services = new ArrayList<>();
		// Admin and TraceSession are managed by the tacplus ServiceManager.
		// Typed Pub/Sub RPCs — always register so the endpoints exist from startup.
		// Kafka may not be initialized yet (it starts after TLS delivery), so each
		// handler lambda checks at call time and returns UNAVAILABLE if not ready.
		ServerServiceDefinition typed = buildLazyTypedBindings();
		ServerServiceDefinition intercepted = ServerInterceptors.intercept(
				typed, Auth.requestIdInterceptor());
		services.add(intercepted);
		return services;
	}

	/**
	 * Builds typed Publish/Subscribe gRPC bindings that resolve Kafka clients
	 * lazily at call time. Returns UNAVAILABLE before Kafka is initialized.
	 */
	private ServerServiceDefinition buildLazyTypedBindings() {
		ServerServiceDefinition.Builder svc = ServerServiceDefinition.builder("eventbus.EventBusTypedService");

		// Trace topics are framework infrastructure (not a vservice); bind them
		// explicitly so they don't self-dispatch traces.
		addTraceBindings(svc,
			TypedEventBusBindings.PUBLISH_TRACE_EVENT_BUS_TRACE_EVENTS,
			TypedEventBusBindings.SUBSCRIBE_TRACE_EVENT_BUS_TRACE_EVENTS);
		addTraceBindings(svc,
			TypedEventBusBindings.PUBLISH_TRACE_TRACE_EVENT_BUS_TRACE_EVENTS,
			TypedEventBusBindings.SUBSCRIBE_TRACE_TRACE_EVENT_BUS_TRACE_EVENTS);

		// Every vservice topic is auto-registered with identity-stamping (C2S) and
		// generic trace dispatch — no per-vservice code. New sensors appear here
		// automatically after codegen rebuild; this handler names no vservice.
		TypedEventBusBindings.addRemainingLazy(svc,
			() -> publisher, () -> subscriber, () -> topicManager,
			this::dispatchTrace,
			TypedEventBusBindings.PUBLISH_TRACE_EVENT_BUS_TRACE_EVENTS,
			TypedEventBusBindings.SUBSCRIBE_TRACE_EVENT_BUS_TRACE_EVENTS,
			TypedEventBusBindings.PUBLISH_TRACE_TRACE_EVENT_BUS_TRACE_EVENTS,
			TypedEventBusBindings.SUBSCRIBE_TRACE_TRACE_EVENT_BUS_TRACE_EVENTS);

		return svc.build();
	}

	/**
	 * Generic trace dispatch: emits an internal-request TraceEvent for the given
	 * trace_id. Passed to the generated bindings so every vservice topic gets
	 * trace dispatch without naming any vservice. Invoked only for non-empty
	 * trace_ids on non-trace topics.
	 */
	private void dispatchTrace(String traceId) {
		watchTraceHandler.dispatch(Trace.TraceEvent.newBuilder()
			.setTraceId(traceId)
			.setKind(Trace.TraceEventKind.TRACE_EVENT_KIND_INTERNAL_REQUEST)
			.setRound(Trace.TraceRound.newBuilder()
				.setService(SERVICE_NAME)
				.setInstanceId(sm.getInstanceId())
				.setTimestampMs(System.currentTimeMillis())
				.build())
			.build());
	}

	private void addTraceBindings(ServerServiceDefinition.Builder svc,
			MethodDescriptor<Trace.TraceEvent, Eventbus.PublishResponse> publishMethod,
			MethodDescriptor<Eventbus.SubscribeRequest, DynamicMessage> subscribeMethod) {
		svc.addMethod(publishMethod,
			ServerCalls.asyncBidiStreamingCall(
				(StreamObserver<Eventbus.PublishResponse> resp) -> {
					if (publisher == null || topicManager == null) {
						resp.onError(Status.UNAVAILABLE.withDescription("Kafka not yet initialized").asRuntimeException());
						return noOpObserver();
					}
					return new PublishHandler<>(TopicRegistry.TOPIC_TRACE_EVENT_BUS_TRACE_EVENTS, publisher, topicManager, resp,
						null, null, null);
				}));

		svc.addMethod(subscribeMethod,
			ServerCalls.asyncBidiStreamingCall(
				(StreamObserver<DynamicMessage> resp) -> {
					if (subscriber == null) {
						resp.onError(Status.UNAVAILABLE.withDescription("Kafka not yet initialized").asRuntimeException());
						return noOpObserver();
					}
					return new SubscribeHandler<>(subscriber, TopicRegistry.TOPIC_TRACE_EVENT_BUS_TRACE_EVENTS, resp,
						Trace.TraceEvent::parseFrom);
				}));
	}

	@SuppressWarnings("unchecked")
	private static <T> StreamObserver<T> noOpObserver() {
		return (StreamObserver<T>) NO_OP;
	}

	private static final StreamObserver<Object> NO_OP = new StreamObserver<>() {
		@Override public void onNext(Object value) {}
		@Override public void onError(Throwable t) {}
		@Override public void onCompleted() {}
	};

	@Override
	public void newTlsCert(X509.X509CertChain certChain, byte[] privateKeyDer) throws Exception {
		logger.info("Received TLS certificate from kernel ({} certs in chain)",
				certChain.getChainCount());
		// First delivery — kick off Kafka initialization (which reads the cert
		// from sm.getTlsCertChain()). Runs in a background thread with retry so
		// we don't block the TLS callback.
		if (!initStarted) {
			initStarted = true;
			Thread initThread = new Thread(() -> initializeWithRetry(), "eventbus-init-retry");
			initThread.setDaemon(true);
			initThread.start();
			return;
		}
		// Subsequent deliveries are renewals — push the new cert to the broker.
		if (broker != null) {
			try {
				PrivateKey pk = sm.getTlsPrivateKey();
				if (pk == null) {
					logger.warn("Renewed TLS cert delivered but SM private key is null — skipping broker update");
					return;
				}
				KafkaEmbeddedBroker.TlsConfig renewed = buildInterNodeTls(certChain, pk);
				broker.updateTls(renewed);
				logger.info("Inter-node TLS cert renewed");
			} catch (Exception e) {
				logger.error("Failed to apply renewed inter-node TLS cert: {}", e.getMessage());
			}
		}
	}

	/**
	 * Attempts initialize() with exponential backoff: 2s, 4s, 8s, 16s, 30s (max).
	 * Retries indefinitely at the max delay until success.
	 */
	private void initializeWithRetry() {
		int attempt = 0;
		while (!initialized) {
			try {
				initialize();
				initialized = true;
				if (attempt > 0) {
					logger.info("EventBus initialization succeeded after {} retries", attempt);
				}
			} catch (Exception e) {
				long delayMs = INIT_RETRY_DELAYS_MS[Math.min(attempt, INIT_RETRY_DELAYS_MS.length - 1)];
				logger.error("EventBus initialization failed (attempt {}, retrying in {}ms): {}",
						attempt + 1, delayMs, e.getMessage());
				attempt++;
				try {
					Thread.sleep(delayMs);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					logger.error("EventBus initialization retry interrupted, giving up");
					return;
				}
			}
		}
	}

	@Override
	public void handleEventbusConfigStateConfig(Eventbus.Config cfg) throws Exception {
		// Config is applied once on startup via initialize(); runtime changes are not supported.
		logger.info("handleEventbusConfigStateConfig: runtime config changes are not applied to a running broker");
	}

	@Override
	public void handleEventbusStateConfig(Eventbus.Config cfg) throws Exception {
		// EventbusState has no config singleton; nothing to do.
	}

	@Override
	public void handleAdminConfig(Eventbus.Config cfg) throws Exception {
		// Admin has no runtime config; nothing to do.
	}

	@Override
	public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.Admin.AdminHandler getAdminGrpcImpl() {
		return new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.Admin.AdminHandler() {
			private void checkReady() throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				if (topicManager == null) throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(
						com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.UNAVAILABLE, "Kafka not yet initialized");
			}
			@Override
			public CollectionRuntime.Page<Eventbus.TopicInfo> listTopics(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.ListTopicsRequest request, CollectionRuntime.PageParams page)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try {
					List<Eventbus.TopicInfo> topics = new ArrayList<>();
					for (TopicManager.TopicInfo info : topicManager.listTopics()) topics.add(toProto(info));
					topics.sort(java.util.Comparator.comparing(Eventbus.TopicInfo::getName));
					return pageByStringKey(topics, page, Eventbus.TopicInfo::getName);
				} catch (ProtoException e) { throw e;
				} catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public Eventbus.TopicInfo getTopicInfo(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.TopicInfoRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try { return toProto(topicManager.getTopicInfo(request.getTopic())); }
				catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public void deleteTopic(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.DeleteTopicRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try { topicManager.deleteTopic(request.getTopic()); }
				catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public Eventbus.TopicInfo updateTopicConfig(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.UpdateTopicConfigRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try { return toProto(topicManager.alterTopicConfig(request.getTopic(), request.getRetentionMs(), request.getPartitionCount())); }
				catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public Eventbus.ConsumerGroupInfo getConsumerGroupLag(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.ConsumerGroupRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try { return toProto(topicManager.getConsumerGroupLag(request.getGroupId())); }
				catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public CollectionRuntime.Page<Eventbus.ConsumerGroupInfo> listConsumerGroups(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.ListConsumerGroupsRequest request, CollectionRuntime.PageParams page)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try {
					List<Eventbus.ConsumerGroupInfo> groups = new ArrayList<>();
					for (TopicManager.ConsumerGroupInfo info : topicManager.listConsumerGroups()) groups.add(toProto(info));
					groups.sort(java.util.Comparator.comparing(Eventbus.ConsumerGroupInfo::getGroupId));
					return pageByStringKey(groups, page, Eventbus.ConsumerGroupInfo::getGroupId);
				} catch (ProtoException e) { throw e;
				} catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public void resetConsumerGroupOffsets(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.ResetConsumerGroupOffsetsRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try {
					KafkaSubscriber.SeekMode mode;
					long value = 0;
					switch (request.getResetToCase()) {
						case OFFSET -> { mode = KafkaSubscriber.SeekMode.OFFSET; value = request.getOffset(); }
						case TIMESTAMP -> { mode = KafkaSubscriber.SeekMode.TIMESTAMP; var ts = request.getTimestamp(); value = ts.getSeconds() * 1000 + ts.getNanos() / 1_000_000; }
						case TO_LATEST -> mode = KafkaSubscriber.SeekMode.LATEST;
						case TO_EARLIEST -> mode = KafkaSubscriber.SeekMode.EARLIEST;
						default -> throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INVALID_ARGUMENT, "reset_to is required");
					}
					topicManager.resetConsumerGroupOffsets(request.getGroupId(), request.getTopic(), mode, value);
				} catch (com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException pe) { throw pe; }
				catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public void deleteConsumerGroup(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					Eventbus.ConsumerGroupRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try { topicManager.deleteConsumerGroup(request.getGroupId()); }
				catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public Eventbus.ClusterHealthResponse getClusterHealth(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				checkReady();
				try {
					TopicManager.ClusterHealth health = topicManager.getClusterHealth();
					Eventbus.ClusterHealthResponse.Builder builder = Eventbus.ClusterHealthResponse.newBuilder()
							.setBrokerCount(health.brokerCount()).setControllerId(health.controllerId());
					for (TopicManager.BrokerInfo bi : health.brokers()) {
						builder.addBrokers(Eventbus.BrokerInfo.newBuilder().setId(bi.id()).setHost(bi.host())
								.setPort(bi.port()).setIsController(bi.isController()).build());
					}
					return builder.build();
				} catch (Exception e) { throw new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
		};
	}

	@Override
	public void handleTraceSessionConfig(Eventbus.Config cfg) throws Exception {
		// TraceSession has no runtime config; nothing to do.
	}

	@Override
	public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.TraceSession.TraceSessionHandler getTraceSessionGrpcImpl() {
		return watchTraceHandler;
	}

	@Override
	public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusStateService.EventbusStateServiceHandler getEventbusStateGrpcImpl() {
		return new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusStateService.EventbusStateServiceHandler() {
			@Override
			public void watchNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					io.grpc.stub.StreamObserver<com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesEntriesResponse> responseObserver)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				responseObserver.onCompleted();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty clearNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesClearRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesSizeResponse sizeNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesSizeRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesSizeResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse putIfAbsentNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesPutRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryResponse queryNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryResponse.getDefaultInstance();
			}
			@Override
			public void watchQueryNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryRequest request,
					io.grpc.stub.StreamObserver<com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryResponse> responseObserver)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				responseObserver.onCompleted();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesKeysResponse keySetNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesKeysRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesKeysResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty deleteNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse getNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse putNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesPutRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty setNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesPutRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse removeNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse containsKeyNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesEntriesResponse getAllNodes(
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller,
					com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetAllRequest request)
					throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus.NodesEntriesResponse.getDefaultInstance();
			}
		};
	}

	@Override
	public com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusConfigStateService.EventbusConfigStateServiceHandler getEventbusConfigStateGrpcImpl() {
		return new com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusConfigStateService.EventbusConfigStateServiceHandler() {
			@Override
			public Eventbus.Config getConfig(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller) throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				return Eventbus.Config.getDefaultInstance();
			}
			@Override
			public Eventbus.Config setConfig(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller, Eventbus.Config request) throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				logger.info("setConfig: runtime config changes are not applied to a running broker");
				return request;
			}
			@Override
			public void watchConfig(com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Caller caller, io.grpc.stub.StreamObserver<Eventbus.Config> responseObserver) throws com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.libs.errors.ProtoException {
				responseObserver.onCompleted();
			}
		};
	}

	@Override
	public void shutdown() {
		logger.info("Shutting down event-bus");
		if (traceDispatchThread != null) {
			traceDispatchThread.interrupt();
		}
		if (traceSession != null)
			traceSession.close();
		if (watchTraceHandler != null)
			watchTraceHandler.shutdown();
		refreshScheduler.shutdownNow();
		if (publisher != null)
			publisher.close();
		if (topicManager != null)
			topicManager.close();
		if (broker != null)
			broker.close();
	}

	// -----------------------------------------------------------------------
	// Admin toProto helpers (used by getAdminGrpcImpl anonymous class)
	// -----------------------------------------------------------------------

	private static Eventbus.TopicInfo toProto(TopicManager.TopicInfo info) {
		Eventbus.TopicInfo.Builder b = Eventbus.TopicInfo.newBuilder()
				.setName(info.name()).setPartitionCount(info.partitionCount()).setRetentionMs(info.retentionMs());
		info.earliestOffsets().forEach(b::putEarliestOffsets);
		info.latestOffsets().forEach(b::putLatestOffsets);
		return b.build();
	}

	private static Eventbus.ConsumerGroupInfo toProto(TopicManager.ConsumerGroupInfo info) {
		Eventbus.ConsumerGroupInfo.Builder b = Eventbus.ConsumerGroupInfo.newBuilder()
				.setGroupId(info.groupId()).setState(info.state()).setTotalLag(info.totalLag());
		for (TopicManager.PartitionLag pl : info.partitionLags()) {
			b.addPartitions(Eventbus.PartitionLag.newBuilder().setTopic(pl.topic()).setPartition(pl.partition())
					.setCommittedOffset(pl.committedOffset()).setEndOffset(pl.endOffset()).setLag(pl.lag()).build());
		}
		return b.build();
	}

	private static <T> CollectionRuntime.Page<T> pageByStringKey(
			List<T> orderedItems, CollectionRuntime.PageParams page, java.util.function.Function<T, String> keyFn)
			throws ProtoException {
		String after = pageCursorKey(page);
		long limit = page == null ? 0 : page.limit;
		if (limit <= 0) {
			return new CollectionRuntime.Page<>(java.util.Collections.emptyList(), null, orderedItems.size());
		}

		List<T> items = new ArrayList<>();
		CollectionRuntime.Cursor nextAfter = null;
		for (T item : orderedItems) {
			String key = keyFn.apply(item);
			if (after != null && key.compareTo(after) <= 0) {
				continue;
			}
			if (items.size() >= limit) {
				nextAfter = cursorFor(keyFn.apply(items.get(items.size() - 1)));
				break;
			}
			items.add(item);
		}
		return new CollectionRuntime.Page<>(items, nextAfter, orderedItems.size());
	}

	private static String pageCursorKey(CollectionRuntime.PageParams page) throws ProtoException {
		if (page == null || page.after == null) {
			return null;
		}
		if (page.after.keys == null || page.after.keys.size() != 1) {
			throw new ProtoException(ProtoException.INVALID_ARGUMENT,
					"page_token cursor does not match the collection order");
		}
		return page.after.keys.get(0);
	}

	private static CollectionRuntime.Cursor cursorFor(String key) {
		return new CollectionRuntime.Cursor(java.util.Collections.singletonList(key));
	}

	// -----------------------------------------------------------------------
	// EventBusServiceHandler implementation
	// -----------------------------------------------------------------------

	private static class EventBusServiceHandlerImpl extends AdminGrpc.AdminImplBase {

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
		public void listTopics(Eventbus.ListTopicsRequest request, StreamObserver<Eventbus.ListTopicsResponse> responseObserver) {
			try {
				List<TopicManager.TopicInfo> topics = topicManager.listTopics();
				topics.sort(java.util.Comparator.comparing(TopicManager.TopicInfo::name));
				int start = pageStart(request.getPageToken());
				int end = pageEnd(start, request.getPageSize(), topics.size());
				Eventbus.ListTopicsResponse.Builder builder = Eventbus.ListTopicsResponse.newBuilder();
				for (TopicManager.TopicInfo info : topics.subList(start, end)) {
					builder.addTopics(toProto(info));
				}
				builder.setTotalCount(topics.size());
				if (end < topics.size()) builder.setNextPageToken(Integer.toString(end));
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void getTopicInfo(Eventbus.TopicInfoRequest request, StreamObserver<Eventbus.TopicInfo> responseObserver) {
			try {
				responseObserver.onNext(toProto(topicManager.getTopicInfo(request.getTopic())));
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void deleteTopic(Eventbus.DeleteTopicRequest request, StreamObserver<com.google.protobuf.Empty> responseObserver) {
			try {
				topicManager.deleteTopic(request.getTopic());
				responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void updateTopicConfig(Eventbus.UpdateTopicConfigRequest request, StreamObserver<Eventbus.TopicInfo> responseObserver) {
			try {
				TopicManager.TopicInfo info = topicManager.alterTopicConfig(
						request.getTopic(), request.getRetentionMs(), request.getPartitionCount());
				responseObserver.onNext(toProto(info));
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void getConsumerGroupLag(Eventbus.ConsumerGroupRequest request, StreamObserver<Eventbus.ConsumerGroupInfo> responseObserver) {
			try {
				responseObserver.onNext(toProto(topicManager.getConsumerGroupLag(request.getGroupId())));
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void listConsumerGroups(Eventbus.ListConsumerGroupsRequest request, StreamObserver<Eventbus.ListConsumerGroupsResponse> responseObserver) {
			try {
				List<TopicManager.ConsumerGroupInfo> groups = topicManager.listConsumerGroups();
				groups.sort(java.util.Comparator.comparing(TopicManager.ConsumerGroupInfo::groupId));
				int start = pageStart(request.getPageToken());
				int end = pageEnd(start, request.getPageSize(), groups.size());
				Eventbus.ListConsumerGroupsResponse.Builder builder = Eventbus.ListConsumerGroupsResponse.newBuilder();
				for (TopicManager.ConsumerGroupInfo info : groups.subList(start, end)) {
					builder.addGroups(toProto(info));
				}
				builder.setTotalCount(groups.size());
				if (end < groups.size()) builder.setNextPageToken(Integer.toString(end));
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void resetConsumerGroupOffsets(Eventbus.ResetConsumerGroupOffsetsRequest request,
				StreamObserver<com.google.protobuf.Empty> responseObserver) {
			try {
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
						responseObserver.onError(Status.INVALID_ARGUMENT
								.withDescription("reset_to is required").asRuntimeException());
						return;
				}
				topicManager.resetConsumerGroupOffsets(
						request.getGroupId(), request.getTopic(), mode, value);
				responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void deleteConsumerGroup(Eventbus.ConsumerGroupRequest request, StreamObserver<com.google.protobuf.Empty> responseObserver) {
			try {
				topicManager.deleteConsumerGroup(request.getGroupId());
				responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
		}

		@Override
		public void getClusterHealth(com.google.protobuf.Empty request, StreamObserver<Eventbus.ClusterHealthResponse> responseObserver) {
			try {
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
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
			} catch (Exception e) {
				responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
			}
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

		private static int pageStart(String pageToken) {
			if (pageToken == null || pageToken.isBlank()) {
				return 0;
			}
			return Math.max(0, Integer.parseInt(pageToken));
		}

		private static int pageEnd(int start, int pageSize, int total) {
			int limit = pageSize <= 0 ? 100 : Math.min(pageSize, 500);
			return Math.min(total, start + limit);
		}
	}

	// -----------------------------------------------------------------------
	// Inter-node mTLS
	// -----------------------------------------------------------------------

	/**
	 * Builds a Kafka TLS config from the cert chain + private key already
	 * delivered to this service via the SM's auto-TLS bidi stream. The
	 * private key matches the chain (both are generated by the SM, signed
	 * by kernel). Kafka requires EC P-256 or RSA — eventbus.proto declares
	 * {@code auto_tls: TLS_KEY_TYPE_ECDSA} which yields a P-256 key.
	 */
	private KafkaEmbeddedBroker.TlsConfig buildInterNodeTls(
			X509.X509CertChain certChain, PrivateKey privateKey) {
		if (certChain == null || privateKey == null) {
			throw new IllegalStateException(
					"buildInterNodeTls called before SM auto-TLS delivered a certificate");
		}
		ImmutableCertChain chain = ImmutableCertChain.from(certChain, privateKey);
		return new KafkaEmbeddedBroker.TlsConfig(
				chain.certChainPem(),
				ImmutableCertChain.privateKeyPem(privateKey),
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
