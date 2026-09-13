# eventbus restart → rebalance → health port flips NOT_SERVING → pod killed

**Date:** 2026-09-12
**Status:** SCOPING ONLY — no code changed. Fix proposals below need a decision before implementation.
**Trigger:** scotty report, item 3 of 3: after an eventbus restart the Kafka consumer group
rebalances, and "the rebalance appears to starve eventbus's own gRPC listener / health port long
enough that a health sidecar SIGKILLs the pod (~30s later)", which restarts and can repeat. It
self-stabilized last time.

**Verdict: this is almost certainly not CPU/thread starvation. It is eventbus's own health check
voting itself dead.** `EventBusHandler.checkHealth()` turns *any single* Kafka AdminClient hiccup
into `INSTANCE_STATUS_UNHEALTHY`, with no consecutive-failure threshold and no grace period, and the
chassis wires that straight through to the gRPC `grpc.health.v1` endpoint that both the launcher and
the kubelet probe. A rebalance/broker-recovery window — which is exactly when eventbus restarts —
is indistinguishable, to this check, from the wedged-KRaft-controller case it was written for.

There is a **second, independent** defect (unbounded blocking Kafka poll on the gRPC request thread,
`KafkaSubscriber.java:686`) that genuinely does hold gRPC application threads during a rebalance. It
is real and should be fixed, but on its own it does not explain a NOT_SERVING health response.

Sibling items: offset commits → `decisions/` (tacplus-generator-owned, no eventbus change); 71
missing state-read grants → `decisions/2026-09-12-missing-state-read-grants.md`. Neither is in the
causal path here (see "Ruled out" below).

---

## 1. The wiring, end to end (all confirmed by reading, not assumed)

**a. Who probes what.**

`Dockerfile:32` sets `ENV HEALTH_PORT=8443`. That is the **main TLS gRPC port**
(`GrpcServer.java:243`, default 8443), not the plaintext probe port. The launcher
(`tacplus/launcher/main.go:107-176`) dials `127.0.0.1:$HEALTH_PORT` with
`credentials.NewTLS(...)` and calls `grpc.health.v1.Health/Check` every `HEALTH_INTERVAL`
(default **30s**), killing the child with `child.Process.Kill()` (SIGKILL, `main.go:169`) once
`Check` has been non-`SERVING` **or errored** continuously for `HEALTH_TIMEOUT` (default **5m**).

Separately, `ServiceManager.java:165-169` starts a *plaintext* health-only listener on
`grpcconfig.health_probe_port`, which eventbus does not override, so it takes the shared default
**8444** (`tacplus/pkg/proto/sys/grpcconfig.proto:191`). That is the port a k8s liveness/readiness
probe would use.

**Crucially, both listeners share one `HealthStatusManager`.** `ServiceManager.java:191` hands
`earlyHealthManager` into the `ServiceManager` constructor, and `ServiceManager.java:654` passes that
same instance into `new GrpcServer(...)`. `GrpcServer.setReady(boolean)` (`GrpcServer.java:161-163`)
sets status on it. So **one `setReady(false)` flips 8443 and 8444 together** — the launcher's TLS
probe and the kubelet's plaintext probe fail at the same instant.

**b. What drives `setReady`.**

`ServiceManager.java:755-758`:

```java
healthReporter.setStatusSink(effectiveStatus -> {
    boolean serving = effectiveStatus != InstanceStatus.INSTANCE_STATUS_UNHEALTHY;
    srv.setReady(serving);
});
```

`HealthReporter` runs one daemon thread (`HealthReporter.java:98-102`, named `health-reporter`) and
`scheduleAtFixedRate(publishAndLog, 10s, 10s)` (`HealthReporter.java:177-183`). Each cycle
(`HealthReporter.java:247-279`) calls `checker.checkHealth()` and then the status sink. So the gRPC
health endpoint is re-evaluated **every 10s from whatever eventbus's `checkHealth()` returns**.

**c. What eventbus's `checkHealth()` does.** `EventBusHandler.java:268-280`:

```java
public InstanceStatus checkHealth() {
    if (!initialized || topicManager == null) {
        return InstanceStatus.INSTANCE_STATUS_HEALTHY;   // startup window only
    }
    try {
        topicManager.getClusterHealth();
        return InstanceStatus.INSTANCE_STATUS_HEALTHY;
    } catch (Exception e) {
        logger.warn("Kafka health check failed — reporting UNHEALTHY: {}", e.getMessage());
        return InstanceStatus.INSTANCE_STATUS_UNHEALTHY;
    }
}
```

`TopicManager.getClusterHealth()` (`TopicManager.java:338-362`) is an AdminClient `describeCluster()`
with **two sequential 10s `.get()` deadlines** (`:345`, `:346`) — worst case **20s of blocking**, and
any `TimeoutException`/`ExecutionException` becomes a `RuntimeException`, hence UNHEALTHY.

**One failed or slow `describeCluster` = NOT_SERVING on both health ports. There is no hysteresis,
no consecutive-failure count, no post-`initialized` grace period.**

## 2. The failure sequence

1. eventbus restarts. Because the Kafka broker is **embedded in the same JVM**
   (`KafkaEmbeddedBroker`, started from `EventBusHandler.initialize()`), the restart is simultaneously
   a broker restart: `__cluster_metadata` replay, topic log recovery, KRaft controller election, and
   `__consumer_offsets` recovery — all in-process.
2. `initialize()` succeeds and sets `initialized = true` (`EventBusHandler.java:353`), so
   `checkHealth()` stops returning the free HEALTHY at `:270` and starts probing Kafka for real.
   Note the ordering: `initialized` flips as soon as the broker object is constructed and the trace
   consumer thread is started — **before** the cluster has actually settled.
3. Every scotty (and other) Subscribe stream reconnects at once. Each one rejoins its consumer group;
   the coordinator runs a rebalance per join. Concurrently the broker is still recovering.
4. Within that window one `describeCluster()` is slow or times out. `checkHealth()` → UNHEALTHY →
   `setStatusSink` → `srv.setReady(false)` → **8443 and 8444 both answer NOT_SERVING**.
5. The kubelet liveness probe on 8444 (typical `periodSeconds: 10, failureThreshold: 3`) trips at
   **~30s** — which matches scotty's observed timing far better than the launcher's 5m default —
   and the kubelet SIGTERM/SIGKILLs the pod. (The launcher would also kill it, just later.)
6. Restart → back to step 1, with a *dirtier* log directory and *more* group churn. That is the loop.
7. It self-stabilizes when a restart happens to land a window where `describeCluster` answers within
   10s for three consecutive probes before the next rebalance. Which is luck, and is exactly why
   scotty calls it fragile.

The behaviour at step 4 is **deliberate** — the comment at `EventBusHandler.java:258-266` says so:
"If the KRaft controller is wedged, describeCluster() will time out and we report UNHEALTHY so k8s
liveness probe fails and restarts the pod — in-place recovery without a human wiping the PVC." The
bug is that the check cannot tell a *wedged* controller (restart is the right answer) from a
*recovering/rebalancing* one (restart is the worst possible answer).

## 3. The second, independent defect: blocking poll on the gRPC request thread

`SubscribeHandler.onNext` (`SubscribeHandler.java:172`) runs on the gRPC server's application
executor. On the first message it calls, **synchronously on that thread**:

```java
Session s = subscriber.createSession(kafkaTopic, params);   // SubscribeHandler.java:229
```

and `createSession` ends with (`KafkaSubscriber.java:684-686`):

```java
consumer.subscribe(Collections.singletonList(topic));
// Initial poll to trigger partition assignment — allow enough time
// for the consumer group rebalance protocol to complete.
consumer.poll(Duration.ofMillis(5000));
```

The codebase **already knows this is not bounded by its argument**. `KafkaSubscriber.java:406-410`,
covering the *other* assignment wait, says verbatim: "The attempt count cannot bound this wait on its
own: `poll(timeout)` does not bound the JoinGroup/SyncGroup handshake, so a group stuck rebalancing
parks the loop inside attempt 1 indefinitely." That loop is protected by `ASSIGNMENT_WATCHDOG`
(`KafkaSubscriber.java:113-118`, armed at `:420-425`). **The `createSession` poll at `:686` has no
watchdog at all.** During a rebalance storm it can hold a gRPC application thread indefinitely, one
per reconnecting subscriber, and `GrpcServer` sets no executor (grep for `executor` in
`GrpcServer.java` returns nothing), so it runs on grpc-java's shared **cached** pool — unbounded
thread growth rather than a clean queue.

This makes `Subscribe` RPCs hang and inflates thread count; it does **not** by itself make
`Health/Check` return NOT_SERVING (the health service has no Kafka dependency, and the pool is
unbounded). It is a genuine bug worth fixing, and it makes everything above worse, but do not
mistake it for the root cause.

## 4. Two smells found while reading, not proposed for fixing here

- **Virtual threads + Kafka 3.9 on Java 21.** `SubscribeHandler.java:116-117` runs every session on
  `Executors.newVirtualThreadPerTaskExecutor()`. The image is `distroless/java21`
  (`Dockerfile:9`) and `maven.compiler.release=21` (`pom.xml:16`); Kafka is 3.9.0 (`pom.xml:20`),
  whose `ConsumerNetworkClient.poll` blocks inside a `synchronized` block. On Java 21 (pre-JEP 491)
  that **pins the carrier thread** for the whole blocking poll. With 5s assignment polls
  (`ASSIGNMENT_POLL_MS`, `KafkaSubscriber.java:69`) and up to 12 attempts (`:52`), N reconnecting
  subscribers pin carriers for up to 60s each. The JDK compensates by growing the scheduler pool, so
  this is thread/GC churn, not deadlock — but virtual threads buy nothing here and cost this. Not in
  the causal path of the health flip; noted so it isn't re-derived later.
- **No `-Xmx`.** `Dockerfile:31` sets G1 flags but no heap cap, so the heap is the container
  default (25% of the limit). Combined with sibling bug #1 (offsets never usefully committed) plus
  `AUTO_OFFSET_RESET=earliest` and `ENABLE_AUTO_COMMIT=false` (`KafkaSubscriber.java:647-648`), every
  reconnecting subscriber replays the topic from the beginning through a `BUFFER`-policy gRPC stream,
  concurrently with in-JVM broker log recovery. If the real symptom turns out to be `Check`
  *erroring* rather than returning NOT_SERVING, this is where to look next.

## 5. The one measurement that settles which mechanism fired

The launcher logs two distinguishable lines (`tacplus/launcher/main.go:163-167`):

- `health: 127.0.0.1:8443 not serving for Xs — killing child`
  → `setReady(false)` fired. **Section 2 is the cause.** Fix = §6 Proposal A.
- `health: 127.0.0.1:8443 unreachable for Xs (<err>) — killing child`
  → the probe never got an answer. Thread/GC starvation. Fix = §6 Proposals B + §4.

If the kill came from the kubelet rather than the launcher, the equivalent evidence is whether 8444
answered `NOT_SERVING` or timed out. Also look for the `Kafka health check failed — reporting
UNHEALTHY:` warning from `EventBusHandler.java:277` in the pod log immediately before the kill — its
presence is direct proof of Section 2.

**Please pull this from the cluster logs before implementing anything.** Everything below is written
assuming "not serving", which the code strongly favours, but it is one grep away from being certain.

## 6. Fix proposals

### Proposal A — give `checkHealth()` hysteresis (recommended; addresses the root cause)

Scope: `EventBusHandler.java:268-280` only. Require **N consecutive** `getClusterHealth()` failures
before reporting UNHEALTHY, instead of one. At the fixed 10s `HealthReporter` cadence, N=3 means a
transient Kafka window of up to ~30s no longer kills the pod, while a genuinely wedged controller is
still restarted — just ~30s later than today, which is well inside the launcher's 5m timeout and
comfortably inside any sane liveness `failureThreshold`.

Implementation sketch (one counter field + a threshold constant; ~10 lines, no new dependency, no
new thread):

```java
private static final int UNHEALTHY_STREAK_THRESHOLD = 3;   // × HealthReporter's 10s cadence
private int clusterHealthFailures;                          // health-reporter thread only
```

increment on catch / reset to 0 on success, and return UNHEALTHY only at or above the threshold
(logging the streak count so the cluster log shows the ramp).

**Size: small. eventbus-only.** No chassis, kernel, proto, or tacplus change. `HealthReporter`'s 10s
period is fixed in generated code (`HealthReporter.java:23`) so the threshold is a reliable
wall-clock figure, but it is a *generated* constant — see OQ-3.

### Proposal B — bound the `createSession` initial poll (addresses §3; independent of A)

Scope: `KafkaSubscriber.java:684-686`, plus its one call site `SubscribeHandler.java:229`.
The mechanism already exists in this file — `ASSIGNMENT_WATCHDOG` + `consumer.wakeup()`, as used at
`KafkaSubscriber.java:420-425`. Either arm the same watchdog around the `createSession` poll, or
drop that poll entirely and let `runLoop`'s already-watchdogged assignment wait
(`KafkaSubscriber.java:411-455`) do the job — it is written to handle an empty initial assignment
(`if (partitions.isEmpty())` at `:413`), so the eager poll at `:686` looks redundant rather than
load-bearing.

**Size: small. eventbus-only.** Needs a test; `ConsumerGroupStarvationTest` is the natural home and
already has an embedded broker with two partitions.

### Proposal C — startup grace period (NOT recommended)

An explicit "healthy for the first T seconds after `initialized`" window was considered and is worse
than A: it only covers the *first* rebalance after a restart, and scotty's report is explicitly that
this "could recur under load", i.e. on rebalances that are not restart-adjacent. A wins on the same
budget. Listed only so it is not re-proposed.

### Deliberately not proposed

- Anything touching grants/OPA/`registry.json` — owned elsewhere, in flight.
- Removing the UNHEALTHY-on-wedged-controller behaviour. It is intentional and load-bearing (see the
  comment at `EventBusHandler.java:258-266`); A tunes it rather than removing it.
- Moving subscribe sessions off virtual threads (§4). Real, but a separate change with its own
  performance argument, and not on this causal path.

## 7. Open questions for a human

**OQ-1 — how long may a wedged KRaft controller go unrestarted?** Proposal A trades restart latency
for restart stability. N=3 (~30s) is my default and what I would implement absent an answer, but the
number is a product/SRE call: it is the maximum time eventbus will serve traffic with a possibly-dead
embedded broker. Someone who knows the SLO should pick it. Worth also deciding whether it should be
an env-var override, matching the `EVENTBUS_SUBSCRIBER_*` pattern already in
`KafkaSubscriber.java:52-101`, rather than a hard constant.

**OQ-2 — what is actually killing the pod, the launcher or the kubelet, and with what thresholds?**
`HEALTH_TIMEOUT` and `HEALTH_INTERVAL` appear **nowhere** in this repo (grepped across all yaml/go/
json outside `target/`), so the launcher is running its defaults: 30s interval, **5m** timeout. That
does not match scotty's "~30s". A kubelet liveness probe on 8444 with the usual
`periodSeconds: 10, failureThreshold: 3` does match exactly. **Where does the eventbus k8s
Deployment/manifest live?** It is not in `platform/services/eventbus/`, and its probe settings decide
whether N=3 in Proposal A is actually sufficient. If `failureThreshold` is 1, no value of N helps and
the manifest must change too.

**OQ-3 — `HealthReporter.REPORT_INTERVAL_MS` is generated, not configurable.**
`HealthReporter.java:23` hardcodes 10s and the class is `CODE GENERATED - DO NOT EDIT`. Proposal A's
threshold is expressed in cycles, so its wall-clock meaning silently changes if tacplus ever changes
that constant. Acceptable? Or should the streak be wall-clock (`System.nanoTime()`-based) inside
eventbus so it is independent of the generated cadence? I lean wall-clock for exactly this reason but
it is marginally more code, and cycles are what the existing comment style in this file assumes.

**OQ-4 — should `checkHealth()` block the health thread for up to 20s at all?**
`TopicManager.getClusterHealth()` (`:345-346`) can hold the single `health-reporter` thread for 20s,
which stretches the 10s publish cadence to ~20-30s and *delays the recovery flip back to SERVING* by
the same amount. Even with Proposal A, the ramp to UNHEALTHY and the ramp back are both slower than
the nominal 10s×N under exactly the conditions that matter. Options: shorten the two `.get()`
deadlines for the health path specifically (they are currently shared with the `Admin.getClusterHealth`
RPC at `EventBusHandler.java:495` and `:873`), or cache the last cluster-health result and refresh it
asynchronously. Both are larger than Proposal A and I did not scope them; flagging because if the
recovery-flip delay turns out to matter in practice, A alone may not be enough.

**OQ-5 — is `initialized = true` set too early?** It flips once `initialize()` returns, which is
after the broker object is constructed and the trace consumer thread is *started* — not after the
cluster is demonstrably serving. That is what ends the free-HEALTHY window at
`EventBusHandler.java:269-271` while recovery is still in progress. Tightening this is an alternative
to Proposal A (or a complement), but "demonstrably serving" needs a definition, which is the same
product question as OQ-1 wearing a different hat.

## 8. Ruled out

- **The 71 missing state-read grants (sibling item 2).** The health *publish* to
  `ServiceHealthState.set_instances` could plausibly be denied, but `HealthReporter.publish()`
  (`:247-279`) calls the status sink **before** publishing, and a publish failure is only logged
  (`publishAndLog`, `:241-246`). A denied publish therefore cannot flip `setReady`.
- **Kafka offset commits (sibling item 1).** Not in the health path. It does amplify the restart cost
  (full replay per reconnecting subscriber — see §4) but cannot produce a NOT_SERVING response.
- **The gRPC application executor filling up.** `GrpcServer` never calls `.executor(...)`, so
  grpc-java's default shared *cached* pool is used — unbounded, so blocked `onNext` calls grow the
  thread count rather than queue new requests behind them.
- **`KafkaSubscriber`'s own rebalance handling.** The poll loop is careful: it keeps polling while
  paused (`:497-520`), has a poll-stall watchdog (`:124-141`), and re-reads the assignment after a
  rebalance before seeking (`:479-483`). `ConsumerGroupStarvationTest` covers both known starvation
  modes. I found nothing new wrong in it.

## 9. What was not done

No code was changed. Proposal A is small enough to implement, but its only real parameter is the
answer to OQ-1, and picking it silently would be guessing at an availability trade-off that belongs
to whoever owns eventbus's SLO. Proposal B is safe to implement independently of that decision if
someone wants it landed now.
