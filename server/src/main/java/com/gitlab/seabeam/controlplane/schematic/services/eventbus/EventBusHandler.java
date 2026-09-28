// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.KafkaEmbeddedBroker;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.KafkaPublisher;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.KafkaSubscriber;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka.TopicManager;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.generated.TopicRegistry;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.generated.TypedEventBusBindings;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.grpc.WatchTraceHandler;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.EventbusStateServiceGrpc;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Helpers;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.host.HostOuterClass;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.x509.X509;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.MetaData;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.collection.CollectionRuntime;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Auth;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.grpcserver.Authorizer;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.x509.ImmutableCertChain;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.Handler;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.ServiceManager;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.healthreport.Healthreport.InstanceStatus;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.AdminGrpc;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.EventbusConfigStateServiceGrpc;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.trace.Trace;
import com.google.protobuf.DynamicMessage;

import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Metadata;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import com.gitlab.seabeam.controlplane.schematic.services.eventbus.grpc.PublishHandler;
import com.gitlab.seabeam.controlplane.schematic.services.eventbus.grpc.SubscribeHandler;

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
	/**
	 * Consecutive failed Kafka health probes before checkHealth() reports
	 * UNHEALTHY. At the health reporter's fixed 10s cadence this is ~240s of
	 * tolerated Kafka unavailability.
	 *
	 * It was 3 (~30s), chosen to match the usual k8s {@code failureThreshold:
	 * 3}. That turned out to be an order of magnitude short of reality: after a
	 * restart every downstream Subscribe RPC reconnects at once (~25 sessions
	 * inside one second), and while the resulting rebalance runs,
	 * {@code describeCluster()} does time out. Measured assignment waits in
	 * that window were 87-135s, so a 30s budget guaranteed the pod was killed
	 * mid-rebalance, every time, which made the next rebalance worse.
	 *
	 * A post-restart rebalance is functionally a second cold start, so this is
	 * sized like one: 240s sits between the real 135s worst case and the 300s
	 * the startup probe already allows, and it trips BEFORE the 300s k8s
	 * liveness probe, so eventbus's own verdict is what restarts the pod and
	 * the kubelet stays the backstop.
	 *
	 * The wedged-KRaft-controller case this check exists for still restarts the
	 * pod, just at ~4 minutes instead of ~30 seconds — a deliberate trade, and
	 * still inside the launcher's 5m HEALTH_TIMEOUT. See
	 * {@code decisions/2026-09-13-assignment-budget-equals-poll-interval.md}.
	 */
	static final int UNHEALTHY_STREAK_THRESHOLD = 24;

	/**
	 * The health reporter's fixed cadence, mirrored from the generated
	 * {@code HealthReporter.REPORT_INTERVAL_MS}. Only used to express the
	 * threshold above as wall-clock time in the tests — if tacplus ever changes
	 * that constant, {@code HealthStreakTest} is what notices.
	 */
	static final long HEALTH_REPORT_INTERVAL_MS = 10_000;

	private final ServiceManager sm;
	private final ScheduledExecutorService refreshScheduler;
	private volatile boolean initialized = false;
	private volatile boolean initStarted = false;
	/** Consecutive failed Kafka health probes; health-reporter thread only. */
	private int clusterHealthFailures = 0;

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
		// Fetch eventbus-specific config from kernel's ConfigHolder, through the
		// GENERATED state client.
		//
		// This used to read sm.getToken() and hand-build an "authorization:
		// Bearer ..." header. That worked, but it duplicates what the chassis
		// already does and it pins one token for the life of the stub — the same
		// staleness that made the node-registration refresh below fail forever.
		// The generated client carries callCredentialsForAudience("kernel"),
		// which is re-evaluated per call.
		var configStub = sm.getStateClients().services().eventbusConfigState().stateStub();
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
		//
		// One ServerServiceDefinition PER TOPIC, under the service names the
		// generated proto declares (<pkg>.<Topic>StateService). The flat
		// broker-qualified service that used to be registered alongside these is
		// gone: it was declared in no proto, so nothing but a hardcoding client
		// could reach it, and every generated caller now emits the per-topic name.
		//
		// Trace topics are included here rather than bound separately. The
		// generated body passes no trace-id extractor for a TraceEvent type, so the
		// trace topic still does not dispatch traces about itself — which is what
		// the old hand-written trace binding existed to arrange.
		//
		// Per-topic AUTHORIZATION. The ServiceManager wraps everything returned
		// here in enforcingInterceptor(), which verifies signature, audience and
		// token class but explicitly does NOT check entitlements — so on its own
		// any holder of an eventbus-audience token could publish to and subscribe
		// from every topic, the platform trace topic included. authorizingInterceptor
		// applies the same check a generated service wrapper performs: the
		// auth_services patterns the generated proto declares per topic for S2S
		// callers, service_entitlements for user callers. Fail-closed — a method
		// with no registered pattern is denied, and the generator refuses to emit
		// a topic that declares none.
		Authorizer authorizer = sm.getAuthorizer();
		if (authorizer == null) {
			throw new IllegalStateException(
					"additionalServices() called before the Authorizer was built — "
					+ "per-topic authorization cannot be enforced");
		}
		TypedEventBusBindings.registerMethodAuth(authorizer);
		// Last interceptor runs first, so authorization precedes request-id tagging.
		ServerInterceptor topicAuth = authorizer.authorizingInterceptor();
		for (ServerServiceDefinition perTopic : TypedEventBusBindings.buildPerTopicLazy(
				() -> publisher, () -> subscriber, () -> topicManager, this::dispatchTrace)) {
			services.add(ServerInterceptors.intercept(perTopic, Auth.requestIdInterceptor(), topicAuth));
		}
		return services;
	}

	/**
	 * Probes the embedded Kafka broker. If the KRaft controller is wedged,
	 * describeCluster() will time out and we report UNHEALTHY so k8s
	 * liveness probe fails and restarts the pod — in-place recovery
	 * without a human wiping the PVC.
	 *
	 * Before Kafka is initialized (waiting for TLS), we report HEALTHY
	 * because the startup probe covers that window.
	 *
	 * A single failed probe is not enough: a consumer-group rebalance or a
	 * broker recovering after a restart makes describeCluster() slow, and that
	 * is expected, not wedged. Only {@link #UNHEALTHY_STREAK_THRESHOLD}
	 * consecutive failures report UNHEALTHY — sized against the real measured
	 * duration of a post-restart rebalance, not against the k8s probe default.
	 */
	@Override
	public InstanceStatus checkHealth() {
		if (!initialized || topicManager == null) {
			// Not yet initialized — startup probe handles this window.
			return InstanceStatus.INSTANCE_STATUS_HEALTHY;
		}
		try {
			topicManager.getClusterHealth();
		} catch (Exception e) {
			return recordProbe(false, e.getMessage());
		}
		String starved = starvedConsumerGroups();
		if (starved != null) {
			return recordProbe(false, starved);
		}
		return recordProbe(true, null);
	}

	/**
	 * Consumption liveness, as opposed to the process liveness above: a
	 * description of the consumer groups eventbus holds an assigned subscribe
	 * session for and which the coordinator reports as having no members, or
	 * null when there are none.
	 *
	 * A broker that answers describeCluster() is not the same thing as a broker
	 * whose consumers are consuming. The failure this exists for leaves the pod
	 * 1/1 Ready, every stream open, the logs clean and nothing delivered — the
	 * only place it shows is here, in the gap between what eventbus believes it
	 * is consuming and what the coordinator says is joined.
	 *
	 * The session gate is what makes this safe to fail the probe on. An Empty
	 * group with growing lag and nobody subscribed is entirely normal — a
	 * subscriber that scaled down or went away — and restarting eventbus for it
	 * would be wrong. A group eventbus is holding assigned partitions in is
	 * eventbus's own to answer for.
	 *
	 * It folds into recordProbe's existing streak rather than adding a second
	 * verdict: a rebalance shows zero members briefly, so this has to persist
	 * for the same ~240s as a wedged broker before it restarts anything.
	 */
	private String starvedConsumerGroups() {
		Set<String> assigned = KafkaSubscriber.assignedConsumerGroups();
		if (assigned.isEmpty()) {
			return null;
		}
		try {
			Map<String, Long> starved = topicManager.groupsWithNoMembers(assigned);
			if (starved.isEmpty()) {
				return null;
			}
			return "consumer group(s) with an assigned eventbus subscribe session but no members "
					+ "(group=total_lag): " + starved;
		} catch (Exception e) {
			// Could not tell. The cluster probe above already passed, so report
			// nothing rather than count a probe failure we cannot attribute.
			logger.warn("Consumer group membership probe failed: {}", e.getMessage());
			return null;
		}
	}

	/**
	 * Folds one probe result into the consecutive-failure streak and returns
	 * the status to report. Called only from the health-reporter thread.
	 */
	InstanceStatus recordProbe(boolean ok, String failureMessage) {
		if (ok) {
			clusterHealthFailures = 0;
			return InstanceStatus.INSTANCE_STATUS_HEALTHY;
		}
		clusterHealthFailures++;
		if (clusterHealthFailures < UNHEALTHY_STREAK_THRESHOLD) {
			logger.warn("Kafka health check failed ({}/{} consecutive) — still reporting HEALTHY: {}",
					clusterHealthFailures, UNHEALTHY_STREAK_THRESHOLD, failureMessage);
			return InstanceStatus.INSTANCE_STATUS_HEALTHY;
		}
		logger.warn("Kafka health check failed ({} consecutive) — reporting UNHEALTHY: {}",
				clusterHealthFailures, failureMessage);
		return InstanceStatus.INSTANCE_STATUS_UNHEALTHY;
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
				// Log the exception, not just getMessage(). For an IOException
				// the message is bare -- an AccessDeniedException's message is
				// only the path -- so this line read
				//   "initialization failed (attempt 131): /app/data/__cluster_metadata-0"
				// with no type and no stack, and gave no hint that the cause was
				// a permission denial on a root-owned volume. It stayed
				// unexplained for 131 retries.
				logger.error("EventBus initialization failed (attempt {}, retrying in {}ms)",
						attempt + 1, delayMs, e);
				// initialize() may have already bound the embedded Kafka broker's
				// listener ports before a later step (topic bootstrap, publisher/
				// subscriber setup) threw. Without closing it here, the next
				// retry's new KafkaEmbeddedBroker tries to bind the same ports
				// and fails immediately with "Address already in use" forever.
				if (broker != null) {
					try {
						broker.close();
					} catch (Exception closeEx) {
						logger.warn("Failed to close broker after failed init attempt: {}", closeEx.getMessage());
					}
					broker = null;
				}
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

	/**
	 * Resolves the Kafka consumer group an Admin RPC may act on, or throws
	 * PERMISSION_DENIED when the caller does not own the group it named.
	 *
	 * Admin is declared {@code auth_services: ["*"]} in the proto, so the
	 * Authorizer lets every registered platform service through and the only
	 * place a per-caller boundary can be drawn is here. The scheme is not a new
	 * one: it is {@link SubscribeHandler#boundConsumerGroup}'s
	 * {@code callerSub|group} binding, read back through
	 * {@link SubscribeHandler#ownedConsumerGroup} -- the same isolation the
	 * subscribe path has always enforced, which Admin previously routed around.
	 *
	 * See {@code decisions/2026-09-13-admin-rpc-caller-scoping.md}.
	 */
	static String adminOwnedGroup(
			com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
			String requested) throws ProtoException {
		String callerSub = caller == null ? "" : caller.getSubject();
		String groupId = SubscribeHandler.ownedConsumerGroup(callerSub, requested);
		if (groupId.isEmpty()) {
			// Deliberately does not say whether the group exists: a caller that
			// does not own it learns nothing about it either way.
			throw ProtoException.newErrPermissionDenied(null,
					"consumer group \"%s\" is not owned by the calling service", requested);
		}
		return groupId;
	}

	@Override
	public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.Admin.AdminHandler getAdminGrpcImpl() {
		return new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.Admin.AdminHandler() {
			private void checkReady() throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				if (topicManager == null) throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(
						com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.UNAVAILABLE, "Kafka not yet initialized");
			}
			@Override
			public CollectionRuntime.Page<Eventbus.TopicInfo> listTopics(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.ListTopicsRequest request, CollectionRuntime.PageParams page)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				checkReady();
				try {
					List<Eventbus.TopicInfo> topics = new ArrayList<>();
					for (TopicManager.TopicInfo info : topicManager.listTopics()) topics.add(toProto(info));
					topics.sort(java.util.Comparator.comparing(Eventbus.TopicInfo::getName));
					return pageByStringKey(topics, page, Eventbus.TopicInfo::getName);
				} catch (ProtoException e) { throw e;
				} catch (Exception e) { throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public Eventbus.TopicInfo getTopicInfo(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.TopicInfoRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				checkReady();
				try { return toProto(topicManager.getTopicInfo(request.getTopic())); }
				catch (Exception e) {
					// A topic Kafka does not know is NOT_FOUND, not INTERNAL: stratum's startup replay
					// (ColdStartRebuilder.awaitWatermarks) treats NOT_FOUND as "nothing to replay" and
					// keeps its read gate closed on anything else.
					for (Throwable t = e; t != null; t = t.getCause()) {
						if (t instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
							throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.NOT_FOUND, "topic not found: " + request.getTopic());
						}
					}
					throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INTERNAL, e.getMessage());
				}
			}
			@Override
			public void deleteTopic(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.DeleteTopicRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				// Topic deletion is entitled to nobody. Admin is declared
				// auth_services: ["*"], which let any registered platform service
				// delete any topic, including another service's; tacplus can
				// express a narrower policy per RPC ((spec.rpc).auth_services),
				// but that list lives in the proto repo, not here. No caller in
				// the fleet invokes DeleteTopic today, so the narrowest list that
				// loses nothing is the empty one, enforced here until the proto
				// says it. See decisions/2026-09-13-admin-rpc-caller-scoping.md.
				throw ProtoException.newErrPermissionDenied(null,
						"DeleteTopic is not entitled to any caller; topics are created from their "
						+ "(spec.topic) declaration and removed by changing that declaration");
			}
			@Override
			public Eventbus.TopicInfo updateTopicConfig(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.UpdateTopicConfigRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				// Same treatment as DeleteTopic, for the same reason: Admin is
				// auth_services: ["*"], so any registered platform service could
				// shrink another service's topic retention -- DeleteTopic's data
				// loss in slow motion -- or repartition it underneath its
				// consumers. Scoping it to caller-owned topics is not available:
				// nothing in the registry says which service owns a topic (a
				// topic carries a declaring package and an auth_services
				// entitlement list, and auth_services is explicitly an identity
				// grant, not a namespace), and inventing an ownership scheme
				// here would be a second, disagreeing answer to a question the
				// platform does not otherwise ask. No caller in the fleet
				// invokes UpdateTopicConfig today, so the narrowest policy that
				// loses nothing is the empty one, enforced here until the proto
				// says it. See decisions/2026-09-13-admin-rpc-caller-scoping.md.
				throw ProtoException.newErrPermissionDenied(null,
						"UpdateTopicConfig is not entitled to any caller; topic retention and "
						+ "partitioning come from eventbus's own default_retention_hours / "
						+ "default_partitions config, not from a per-caller RPC");
			}
			@Override
			public Eventbus.ConsumerGroupInfo getConsumerGroupLag(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.ConsumerGroupRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				// Before checkReady(): a caller that owns nothing here learns
				// nothing about the broker's state either. Lag answers for an
				// arbitrary group id leaked per-group state (members, assigned
				// topics, offsets) to any caller that could guess a group id,
				// which is what ListConsumerGroups' scoping alone does not stop.
				String groupId = adminOwnedGroup(caller, request.getGroupId());
				checkReady();
				try { return toProto(topicManager.getConsumerGroupLag(groupId)); }
				catch (Exception e) { throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public CollectionRuntime.Page<Eventbus.ConsumerGroupInfo> listConsumerGroups(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.ListConsumerGroupsRequest request, CollectionRuntime.PageParams page)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				checkReady();
				// Scoped to the caller's own groups. A fleet-wide list has no
				// caller today and would disclose every service's SPIFFE subject
				// (group ids carry it) to anything holding an eventbus token.
				String callerSub = caller == null ? "" : caller.getSubject();
				try {
					List<Eventbus.ConsumerGroupInfo> groups = new ArrayList<>();
					for (TopicManager.ConsumerGroupInfo info : topicManager.listConsumerGroups()) {
						if (!SubscribeHandler.ownsConsumerGroup(callerSub, info.groupId())) continue;
						groups.add(toProto(info));
					}
					groups.sort(java.util.Comparator.comparing(Eventbus.ConsumerGroupInfo::getGroupId));
					return pageByStringKey(groups, page, Eventbus.ConsumerGroupInfo::getGroupId);
				} catch (ProtoException e) { throw e;
				} catch (Exception e) { throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public void resetConsumerGroupOffsets(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.ResetConsumerGroupOffsetsRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				// Before checkReady(): a caller that owns nothing here learns
				// nothing about the broker's state either.
				String groupId = adminOwnedGroup(caller, request.getGroupId());
				checkReady();
				try {
					KafkaSubscriber.SeekMode mode;
					long value = 0;
					switch (request.getResetToCase()) {
						case OFFSET -> { mode = KafkaSubscriber.SeekMode.OFFSET; value = request.getOffset(); }
						case TIMESTAMP -> { mode = KafkaSubscriber.SeekMode.TIMESTAMP; var ts = request.getTimestamp(); value = ts.getSeconds() * 1000 + ts.getNanos() / 1_000_000; }
						case TO_LATEST -> mode = KafkaSubscriber.SeekMode.LATEST;
						case TO_EARLIEST -> mode = KafkaSubscriber.SeekMode.EARLIEST;
						default -> throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INVALID_ARGUMENT, "reset_to is required");
					}
					topicManager.resetConsumerGroupOffsets(groupId, request.getTopic(), mode, value);
				} catch (com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException pe) { throw pe; }
				catch (Exception e) { throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public void deleteConsumerGroup(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					Eventbus.ConsumerGroupRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				// Before checkReady(): a caller that owns nothing here learns
				// nothing about the broker's state either.
				String groupId = adminOwnedGroup(caller, request.getGroupId());
				checkReady();
				try { topicManager.deleteConsumerGroup(groupId); }
				catch (Exception e) { throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
			@Override
			public Eventbus.ClusterHealthResponse getClusterHealth(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
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
				} catch (Exception e) { throw new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException.INTERNAL, e.getMessage()); }
			}
		};
	}

	@Override
	public void handleTraceSessionConfig(Eventbus.Config cfg) throws Exception {
		// TraceSession has no runtime config; nothing to do.
	}

	@Override
	public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.TraceSession.TraceSessionHandler getTraceSessionGrpcImpl() {
		return watchTraceHandler;
	}

	@Override
	public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusStateService.EventbusStateServiceHandler getEventbusStateGrpcImpl() {
		return new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusStateService.EventbusStateServiceHandler() {
			@Override
			public void watchNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					io.grpc.stub.StreamObserver<com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesEntriesResponse> responseObserver)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				responseObserver.onCompleted();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty clearNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesClearRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesSizeResponse sizeNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesSizeRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesSizeResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse putIfAbsentNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesPutRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryResponse queryNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryResponse.getDefaultInstance();
			}
			@Override
			public void watchQueryNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryRequest request,
					io.grpc.stub.StreamObserver<com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesQueryResponse> responseObserver)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				responseObserver.onCompleted();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesKeysResponse keySetNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesKeysRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesKeysResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty deleteNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse getNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse putNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesPutRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty setNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesPutRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.MapEmpty.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse removeNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse containsKeyNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesValueResponse.getDefaultInstance();
			}
			@Override
			public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesEntriesResponse getAllNodes(
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller,
					com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesGetAllRequest request)
					throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus.NodesEntriesResponse.getDefaultInstance();
			}
		};
	}

	@Override
	public com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusConfigStateService.EventbusConfigStateServiceHandler getEventbusConfigStateGrpcImpl() {
		return new com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.EventbusConfigStateService.EventbusConfigStateServiceHandler() {
			@Override
			public Eventbus.Config getConfig(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller) throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				return Eventbus.Config.getDefaultInstance();
			}
			@Override
			public Eventbus.Config setConfig(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller, Eventbus.Config request) throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
				logger.info("setConfig: runtime config changes are not applied to a running broker");
				return request;
			}
			@Override
			public void watchConfig(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller caller, io.grpc.stub.StreamObserver<Eventbus.Config> responseObserver) throws com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException {
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
		public void getClusterHealth(com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.trace.Trace.TraceRequest request, StreamObserver<Eventbus.ClusterHealthResponse> responseObserver) {
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

		// The GENERATED state client, not a raw stub off the channel.
		//
		// sm.getStateClients() hands back a stub with
		// callCredentialsForAudience("kernel") already attached. A stub built
		// straight from the channel carries no credentials at all, so kernel
		// rejected every call with "missing authorization header" — the node
		// never registered, and the refresh below repeated the same rejection
		// once a minute, indefinitely.
		var stateStub = sm.getStateClients().services().eventbusState().stateStub();

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
				// Re-fetched each tick rather than captured: the client rebinds
				// its stub (and credentials) when the channel is re-established.
				sm.getStateClients().services().eventbusState().stateStub().setNodes(putRequest);
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
