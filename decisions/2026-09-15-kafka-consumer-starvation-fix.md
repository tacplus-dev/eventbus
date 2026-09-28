# Kafka consumer starvation: the subscribe poll loops ran on virtual threads

**Date:** 2026-09-15
**Status:** IMPLEMENTED — eventbus changed, tests added, uncommitted for review.
**Live status:** root cause confirmed on the running pod and the fix verified live, but the
cluster was **rolled back** to its pre-existing image and the outage is still live. The
image I could build here is missing scotty's topic bindings; a correct one needs the pinned
tacplus checkout. Read §4 "Live" before acting on this.
**Trigger:** live incident. scotty's looper (iter3963,
`scotty/backend/looper/workers/assertion11-regression-result.md`) found every stratum
consumer group in the `schematic` dev cluster `Empty`, every member evicted on
**heartbeat expiration**, and not one rejoined in 25 hours. `kubectl get pods` showed
`1/1 Running` throughout, the gRPC `SubscribeToStream` streams stayed open on both sides,
and stratum's `subscribeOnce` retry loop never fired. assertion 11 of `verify-scotty.sh`
fails because nothing materialises into the record store.
**Follows:** `decisions/2026-09-13-assignment-budget-equals-poll-interval.md`. That fix was
correct and is not implicated — it stopped a crash loop caused by the assignment budget
equalling `max.poll.interval.ms`. This is a different failure that happens *after* a group
stabilizes, and it was present the whole time underneath.

---

## 1. Root cause

`SubscribeHandler` ran every Kafka consumer poll loop on a **virtual thread**:

```java
private static final ExecutorService SUBSCRIBE_EXECUTOR =
        Executors.newVirtualThreadPerTaskExecutor();   // SubscribeHandler.java:117
```

That is the whole bug. A Kafka consumer poll loop is the precise shape virtual threads are
not for:

- it is long-lived and effectively never idle;
- the classic consumer does its blocking inside `synchronized` coordinator methods
  (`AbstractCoordinator.ensureActiveGroup`, `joinGroupIfNeeded`, `ConsumerCoordinator.poll`),
  which **pins** the carrier — a pinned virtual thread cannot unmount;
- a pinned virtual thread that then waits on a lock does not park the *thread*, it parks
  **on its carrier** (`VirtualThread.parkOnCarrierThread`), holding that carrier for the
  entire wait;
- the carrier pool starts at one per CPU, and the GKE nodes this fleet runs on have **2**.

The lock they all wait on is logback's single `OutputStreamAppender` stream-write lock.
Every Kafka client log line goes through it, and the Kafka client logs prolifically from
inside the coordinator monitor.

So: ~25 subscribe sessions, each logging from inside a `synchronized` coordinator frame,
serialize on one appender lock, and each one that waits burns a carrier instead of
yielding it. The scheduler expands the pool trying to compensate, cannot keep up, and
sessions submitted to `SUBSCRIBE_EXECUTOR` stop being scheduled **at all**. A session that
is never scheduled never calls `poll()`. Its consumer never heartbeats. `session.timeout.ms`
is 30000, so the coordinator evicts it **exactly 30s** after the group stabilizes —
which is precisely the period scotty's looper measured across 7+ generations.

### Verified live, on the wedged pod

`eventbus0-7d859dbc59-xkkd8`, up 2d2h, `1/1 Running`. Thread dumps taken with
`jcmd Thread.print` and `jcmd Thread.dump_to_file` through an ephemeral
`eclipse-temurin:21-jdk` debug container sharing the pod's PID namespace (the eventbus
image is distroless — no shell, no `kubectl exec`).

**37 threads parked on one lock**, none holding it, every one of them at the identical
frame `ch.qos.logback.core.OutputStreamAppender.writeBytes(OutputStreamAppender.java:211)`:

| count | thread kind | where |
|---|---|---|
| 16 | virtual | `KafkaSubscriber$Session.runLoop:599` ← `run:459` ← `SubscribeHandler.lambda$onNext$0:238` |
| 3 | virtual | `KafkaSubscriber$Session.runLoop:506` (assignment wait) |
| 1 | virtual | `KafkaSubscriber.applySeek:810` |
| 11 | `grpc-default-executor-N` | `KafkaSubscriber.createSession:749/759/774` ← `SubscribeHandler.onNext:229` |
| **6** | **`kafka-coordinator-heartbeat-thread`** | inside the Kafka client's own logging |

The 12 of those virtual threads whose stack begins
`Unsafe.park → VirtualThread.parkOnCarrierThread → VirtualThread.park → … ReentrantLock.lock`
are the pinned ones — that frame is the JDK telling us the carrier is being held, not
released.

**Six of the blocked threads are Kafka's own heartbeat threads.** That is the direct answer
to "why do the consumers stop heartbeating": they are not stopping by themselves, they are
stuck behind a pinned virtual thread in the FIFO queue of a logging lock.

The carrier pool had expanded to **27** `ForkJoinPool-1-worker` threads, 15 of them
`Carrying virtual thread #N` with ~0ms of CPU consumed over the pod's 106768s lifetime —
carriers held by threads that are doing nothing.

### The same thing in the pod's own log

`runLoop` logs `"runLoop starting: topic=…"` as its very first statement. Counting the
whole log:

```
consumers created (ConsumerConfig values): 42
runLoop started:                           28
sessions ended:                             1
```

Fourteen sessions were created and **never ran a single line of `runLoop`**. The terminal
event is unambiguous:

```
22:33:44.567 … 22:33:45.133   14 x ConsumerConfig values      (stratum-1/-2 restart)
22:33:49.762 … 22:33:50.163   12 x "Initial assignment poll of 5000ms exceeded, waking consumer"
22:33:54.997   Stabilized group …|stratum generation 429 with 4 members
22:34:14.858   Pending member …stratum-31… removed after session timeout expiration
22:34:24.998   Member …stratum-42… has failed, removing it (heartbeat expiration)
22:34:25.001   Group …|stratum with generation 430 is now empty
                      ← and nothing from KafkaSubscriber for the next 25 hours
```

The consumers joined their groups on the **gRPC request thread**, inside
`createSession`'s initial poll. Then `SUBSCRIBE_EXECUTOR.submit()` handed the poll loop to
a virtual thread that was never scheduled, so nothing ever polled or heartbeat again.
Thirty seconds later the coordinator evicted all of them.

Degradation was progressive, which is why this looked intermittent for two days —
measured delay between a session's `ConsumerConfig values` line and its `runLoop starting`:

```
18:28:59 create -> 18:29:04 run     5s
18:33:04 create -> 18:34:01 run    57s
22:33:44 create -> never
```

The earlier era shows the same mechanism short of terminal, as repeated
`Consumer poll loop STALLED … no successful poll for 62000-77000ms` across every topic at
once — global, not per-topic, which is what a scheduler problem looks like and a Kafka
problem does not.

### Why the previous doc ruled this out, and was wrong

`2026-09-13`'s §5 listed "virtual threads pinning carriers on Java 21" as a known smell and
said it was "still real and still not on this causal path". It was the causal path. The
09-13 incident was genuinely the timeout coincidence; this one was underneath it the whole
time and only became terminal once enough sessions accumulated.

## 2. What changed

### `grpc/SubscribeHandler.java` — the root-cause fix

```java
static final ExecutorService SUBSCRIBE_EXECUTOR =
        Executors.newCachedThreadPool(
                Thread.ofPlatform().daemon().name("eventbus-subscribe-", 0).factory());
```

Platform threads. A poll loop's blocking becomes its own problem instead of the
scheduler's, `synchronized` frames pin nothing, and no session can be starved of a thread
by another session's contention. `newCachedThreadPool` rather than
`newThreadPerTaskExecutor` only so an idle thread is reclaimed after a session ends;
sessions are long-lived, so in steady state it is one thread per session either way.

It is `static` (package-private) rather than `private` so
`SubscribeHandlerTest.subscribeSessionsRunOnPlatformThreadsNotVirtualThreads` can assert
what kind of thread it hands out. Nothing at the call site changes when this line changes,
so a test is the only thing that would notice it going back — this is the same reasoning
that made `validateTimeoutBudgets` package-private in the 09-13 change.

### `kafka/KafkaSubscriber.java` + `grpc/SubscribeHandler.java` — make the failure loud

Before: a consumer that fell out of its group left the gRPC stream open, so the client saw
no error and no events and could not tell the difference between that and an idle topic.
The poll-stall watchdog logged an ERROR and nothing acted on it.

Now the watchdog **fails the session**, and does it **from its own thread**:

- `Session.onStalled(Consumer<String>)` — a callback the gRPC layer registers.
  `SubscribeHandler` passes `this::failStalled`, which terminates the stream with
  `UNAVAILABLE`, so stratum's existing `subscribeOnce` retry loop fires.
- `checkPollStall` invokes it directly, in addition to setting `closed` and calling
  `consumer.wakeup()`.

**The callback is the point, and it is not decoration.** Terminating the stream from the
poll loop — noticing a flag, catching a `WakeupException` — assumes the poll loop still
runs. In this incident the poll loop *is* the stuck thread; in the thread dump above, 20 of
them were parked with no way to reach any check. Recovery driven from the stuck thread
cannot fire in the one case that matters. `ConsumerGroupStarvationTest` mode 3 pins this:
it blocks the handler forever and asserts the stream is failed **while the poll loop is
still stuck inside it**.

`run()` also throws a new `ConsumerStalledException` if the loop does exit after a stall,
which covers the internal trace-dispatch session that registers no callback.

Only sessions that have already been **assigned** partitions are failed. A session still
inside the assignment wait is legitimately not polling for up to the full 180s budget —
three times `POLL_STALL_WARN_MS` — and is governed by its own deadline watchdog; failing it
on the stall threshold would kill every session that sits through a slow rebalance, i.e.
re-create the 09-13 incident. Before assignment the watchdog still logs, with a message
saying which deadline actually governs.

The threshold is unchanged at 60s, and that is now load-bearing rather than arbitrary:
`session.timeout.ms` is 30000, so a session that trips it is not *at risk* of eviction, it
**has already been evicted**. `SESSION_TIMEOUT_MS` was extracted from the inline `"30000"`
in `createSession` so the watchdog can say so.

### `EventBusHandler.checkHealth` + `kafka/TopicManager.java` — consumption liveness

`checkHealth()` probed `describeCluster()`. A broker that answers `describeCluster()` is
not the same thing as a broker whose consumers are consuming, and this failure mode lives
entirely in that gap: pod Ready, streams open, logs clean, nothing delivered.

`checkHealth()` now also asks: **of the consumer groups eventbus holds an assigned
subscribe session for, does the coordinator report any with no members?**

- `KafkaSubscriber.assignedConsumerGroups()` — the groups behind sessions that hold
  partitions. `LIVE_SESSIONS` already existed for the stall watchdog; this reads it.
- `TopicManager.groupsWithNoMembers(groups)` — one `describeConsumerGroups` on just those
  groups, 5s deadline, plus the group's total lag for the log line.

The **session gate is what makes this safe to fail the probe on**. An `Empty` group with
growing lag and nobody subscribed is completely normal — a subscriber that scaled down or
went away — and restarting eventbus for it would be wrong. A group eventbus is holding
*assigned partitions* in is eventbus's own to answer for.

It folds into `recordProbe`'s existing streak rather than adding a second verdict, so it
inherits the ~240s tolerance the 09-13 change sized: a rebalance shows zero members
briefly, and this must persist as long as a wedged broker must before it restarts
anything. No new health mechanism, no new probe, no new k8s wiring.

## 3. What was NOT changed, and why

**Nothing about logback.** The appender lock is the contention point, and making it async
(`AsyncAppender`) or cutting the Kafka client to WARN would both reduce it. Neither is the
bug: with platform threads, contention on that lock is ordinary contention that resolves,
because a waiting thread no longer takes the scheduler down with it. Changing logging
levels would also cost exactly the diagnostics that made this incident solvable — the
`runLoop starting` / `Waiting for partition assignment` lines are what proved the sessions
were never scheduled. See OQ-2.

**Nothing about the 09-13 timeout constants.** `ASSIGNMENT_MAX_ATTEMPTS`,
`MAX_POLL_INTERVAL_MS`, `POLL_STALL_WARN_MS`, `UNHEALTHY_STREAK_THRESHOLD` and the operator's
probe budgets are all unchanged and all still correct. That fix is not implicated.

**`Thread.startVirtualThread` in the existing tests.** `ConsumerGroupStarvationTest` modes 1
and 2 run their session loops on virtual threads. They pass, they are testing something
else, and changing them is not this fix.

**stratum's two consumer-group-id schemes — FLAGGED, NOT FIXED, as asked.** stratum runs
`…/stratum|stratum` for scotty/scottyagent/messaging/work but per-pod
`…/stratum|stratum-stratum-{0,1,2}` for mailbox/accountingrecord/agentsensor. Two schemes
for record types with what look like identical fan-out semantics; almost certainly a
generator inconsistency in the `subscribe` emitter, not a hand-written difference. It is
**not** the cause here — all four groups were equally dead, and all four recover the same
way. Worth a human look at the emitter. See OQ-1.

## 4. Verification

### Tests

`mvn -o test`, full suite:

```
Event Bus Codegen ... Tests run: 30, Failures: 0, Errors: 0, Skipped: 0
Event Bus Server  ... Tests run: 85, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

New:

- `grpc/SubscribeHandlerTest.subscribeSessionsRunOnPlatformThreadsNotVirtualThreads` —
  asserts the executor hands out a non-virtual daemon thread. This is the regression test
  for the root cause.
- `kafka/ConsumerGroupStarvationTest.aStalledPollLoopFailsItsStreamFromTheWatchdogThread`
  (mode 3, against a real embedded broker) — a handler that never returns; asserts the
  stall callback fires, the stall is counted, and `loop.isAlive()` is still true when it
  does. That last assertion is the one that matters: it proves the stream was failed
  *without* the stuck thread's cooperation. Observed in the run:

  ```
  ERROR KafkaSubscriber -- Consumer poll loop STALLED on topic test.starvation.Events:
    no successful poll for 74251ms (warn after 60000ms, session.timeout.ms=30000).
    The coordinator has already dropped this member, so the stream behind it is deaf —
    failing the session so the client reconnects.
  ```

### The repo does not build at HEAD, for an unrelated reason — see OQ-3

```
[ERROR] … codegen:generate-eventbus-rpcs … failed: eventbus topics with no auth_services
        in the registry: [tasks.DelegationProbeBusStateService
        (services.tasks.tasks.DelegationProbeBus.Stream)]
        — declare (spec.service_s2s) auth_services on the topic's state service
```

Confirmed pre-existing: reverting all four of my files and re-running `mvn -o compile`
reproduces it byte-identically. It comes from `model/target/generated-sources/model/registry.json`
(tacplus output, regenerated 2026-09-14 08:14) gaining a `tasks.DelegationProbe` C2S topic
whose state service declares no `auth_services`. The 09-13 doc records a fully green suite
on 2026-09-13, i.e. before that regeneration. **The eventbus build has been broken since
2026-09-14 for everyone.**

Per the standing rule against working around a generator, this was **not** fixed here — the
missing `(spec.service_s2s) auth_services` belongs on the `tasks` service's proto, which is
another repo. To run the suite at all, the single offending eventbus boundary was pruned
from the local generated `registry.json` (a gitignored build artifact under `model/target/`,
not a source file; no tracked file was touched). Every test result above is against a
registry identical to HEAD's except for that one topic, which no code in this change reads.

### Live: the fix works, and the image it was verified with had to be rolled back

**The fix is confirmed against the running cluster. The cluster is NOT left running it.**
Read both halves of this section.

The change was built, pushed to `:local` and rolled out to `eventbus0`, then stratum was
restarted. Before, from the wedged pod's whole log: 42 consumers created, 28 poll loops
ever started, 14 never scheduled. After:

```
consumers created (ConsumerConfig values): 28
runLoop started:                           28
```

**Every session was scheduled.** Group state over the next six minutes — the failure window
was 30 seconds, and this is twelve times that:

```
T+60s   …|stratum  state=Stable   …|stratum-stratum-0  state=Stable
T+120s  …|stratum  state=Stable   …|stratum-stratum-0  state=Stable
T+180s  …|stratum  state=Stable   …|stratum-stratum-0  state=Stable
T+240s  …|stratum  state=Stable   …|stratum-stratum-0  state=Stable
T+300s  …|stratum  state=Stable   …|stratum-stratum-0  state=Stable
T+360s  …|stratum  state=Stable   …|stratum-stratum-0  state=Stable

eventbus log, "is now empty" OR "heartbeat expiration" events since restart:  0
eventbus log, "STALLED" OR "Consumer stalled" events since restart:           0
```

Groups stabilized at 4 → 6 → 8 → 10 members and stayed there. Against the old image every
group went `Empty` on heartbeat expiration ~30s after every single join, across 7+
generations. **Zero evictions, zero stalls, over twelve failure windows.** That is the
root-cause fix working.

### …and then `verify-scotty.sh` was still 10/11, for a reason I caused

```
==> 11/11 an artifact exists for the ticket
  FAIL: no ScottyActivity row for ticket 1789519641871-000094-86f577e1 after ~100s
passed: 10   failed: 1
```

The consumer groups were healthy; the scotty topic was not being consumed at all, because
**the image I built does not serve scotty's topics**:

```
stratum-1: scotty.stream subscription failed (gRPC status=UNIMPLEMENTED), retrying in 30000ms:
           UNIMPLEMENTED: Method not found: scotty.ScottyActivityBusStateService/SubscribeToStream
```

I built it with `schematic/scripts/build-fleet.sh eventbus`, against the `model/target`
registry that happened to be on disk. That registry is **platform-only**:

```
topics in the local registry: AccountingRecordBus, AgentEventBus, AgentSessionStateBus,
                              MailboxEntryBus, MessageBus, TaskActivityBus, TaskEventBus,
                              TraceEventBus
```

No `ScottyActivityBus`, no `AgentCycleBus`, no `WorkActivityBus`. eventbus for this cluster
must be built by **`scotty/backend/build.sh`**, which regenerates every platform service
with `tacplus build --src <platform proto> --src <scotty proto>` — the app's protos change
the generated types *inside* platform services, which is the whole premise of that script's
header. `build-fleet.sh` alone cannot produce a correct eventbus for a cluster running an
application. My image silently dropped three topic bindings, and five of eight consumer
groups worked only because those five are platform topics.

Rebuilding it correctly is blocked and **was not forced**:

```
FAIL /Users/jody/workspace/gitlab/seabeam/schematic/tacplus is not at the pinned generator commit
     expected: b46e8ac10cef6ffd803db3343f2fa9909f9e5407
     actual:   31e3e62c8f694009cc70a08d42023f7bc11614de
```

`build.sh` offers a `TACPLUS_PINNED_SHA=` override. Using it would generate eventbus
against a different generator surface than the rest of the fleet was built against, which
is the exact thing that guard exists to stop and has already burned this codebase twice
(oauth's RecordQuery plane, tasks's TimeseriesKeyQuery plane). Checking out the pinned
commit means writing to `schematic/tacplus`, another repo, possibly with work in flight.
Neither is mine to do.

### Rollback performed

The cluster is restored to exactly the image it was running before I touched it:

```
:local retagged back to sha256:0dc7eb727142c74ea076da3ada3f0d18b2053747482cdd0c9926fa4455839589
       (2026-09-13 21:13 build — the previous :local, recovered by digest from
        Artifact Registry and re-tagged with `docker buildx imagetools create`,
        so the operator's Helm-managed :local reference cannot undo it)

eventbus0 pod imageID: …/schematic/eventbus@sha256:0dc7eb7271…  ✓
runLoop starting by topic: ScottyActivityBus ✓ AgentCycleBus ✓ WorkActivityBus ✓
stratum-1 UNIMPLEMENTED count: 0
```

stratum was restarted onto it. **The cluster is back in the state this task found it in,
including the original bug** — 37 consumers created, 18 poll loops started on the current
pod, so the starvation is live again and assertion 11 will keep failing until a correctly
built image is deployed.

**What is needed to actually close this incident**, by someone who can move or match the
tacplus pin:

```
git -C schematic/tacplus checkout b46e8ac10cef6ffd803db3343f2fa9909f9e5407
cd scotty/backend && ./build.sh eventbus         # generates with platform + scotty protos
kubectl rollout restart deploy/eventbus0 -n schematic
kubectl rollout restart statefulset/stratum -n schematic
./verify-scotty.sh                                # expect 11/11
```

The code change itself is verified: unit + integration tests green, and the live run above
showed the exact behaviour change on the five topic groups that image did serve. What is
unverified is assertion 11 end-to-end, because no image I am able to build correctly
carries scotty's topics.

## 5. Open questions

**OQ-1 — stratum's two consumer-group-id schemes.** `…|stratum` for
scotty/scottyagent/messaging/work vs per-pod `…|stratum-stratum-N` for
mailbox/accountingrecord/agentsensor, for record types that look equivalent. Flagged by
scotty's looper, confirmed in eventbus's own broker log. Not the cause of this incident and
not touched. Someone who owns the `subscribe` emitter should say whether the split is
intentional; if it is not, the per-pod scheme is probably the wrong one, since it prevents
partition sharing across stratum's replicas. **Default proceeded on: unchanged.**

**OQ-2 — should logback's appender be async?** The single `OutputStreamAppender` write lock
is the JVM-wide serialization point every Kafka client log line goes through, and this
incident only needed it to be *contended*, not slow. Platform threads make that contention
survivable rather than fatal, so this is no longer urgent — but the appender is still a
global lock on the hot path of every poll loop, and a genuinely slow stdout (a blocked
container log pipe) would still back all of them up behind it. An `AsyncAppender` with a
bounded, discarding queue would decouple it, at the cost of losing log lines under
pressure — during an incident, exactly when you want them. Not done. **Default proceeded
on: synchronous appender, unchanged.**

**OQ-3 — `tasks.DelegationProbeBus` has no `auth_services`, and it breaks the eventbus
build.** Since the 2026-09-14 model regeneration, `mvn package` on eventbus fails at
`codegen:generate-eventbus-rpcs`. This is a hard, deliberate failure —
`TypedRpcGenerator.collectMethodAuth` refuses to emit a topic binding with no authorization
policy, which is right: serving it would mean serving an eventbus topic with no policy at
all. The missing declaration belongs on the `tasks` service's proto
(`(spec.service_s2s) auth_services` on `DelegationProbeBusStateService`), which is outside
this repo, so it was **not** worked around in any tracked file. Whoever added the item-6
`DelegationProbe` C2S record needs to finish it. Until then eventbus cannot be built or
imaged from a clean tree. **Default proceeded on: reported, not fixed; local generated
registry pruned only to run the tests.**

**OQ-4 — `checkHealth()` can now hold the health-reporter thread for up to 25s.** Carried
forward and made slightly worse: `getClusterHealth()` already had two sequential 10s `.get()`
deadlines (09-13's OQ-4), and the membership probe adds a 5s one. It is still well inside
the 10s report cadence in the normal case — the probe only costs a round trip when it
actually runs — but under a struggling broker the ramp to UNHEALTHY *and the ramp back to
SERVING* both stretch. The deadline was set to 5s rather than 10s for exactly this reason.
The real fix is to make `checkHealth` non-blocking with a cached last-probe result, which is
a larger change than this incident justifies. **Default proceeded on: 5s deadline, still
synchronous.**

**OQ-5 — should `SESSION_TIMEOUT_MS` be env-overridable like the other subscriber
timeouts?** It is a hard constant, while `ASSIGNMENT_*`, `MAX_POLL_INTERVAL_MS`,
`POLL_STALL_WARN_MS` and `MAX_POLL_RECORDS` all take `EVENTBUS_SUBSCRIBER_*` overrides. It
is now load-bearing for the stall watchdog's semantics — the watchdog only claims "already
evicted" because `POLL_STALL_WARN_MS` (60s) exceeds it (30s) — and `validateTimeoutBudgets`
does not check that relation. Making it overridable without also validating it would let a
pod be configured where the watchdog's message is false. **Default proceeded on: hard
constant.**
