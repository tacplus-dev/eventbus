# eventbus CrashLoopBackOff: the assignment budget equalled max.poll.interval.ms

**Date:** 2026-09-13
**Status:** IMPLEMENTED — eventbus + operator changed, tests added, uncommitted for review.
**Trigger:** live incident. `eventbus0-7d58c67b5b-z2mhb` in `schematic` was in CrashLoopBackOff
with 7 restarts and `back-off 5m0s` when this was picked up, ~35 minutes after the pod was created.
**Supersedes the fix in:** `decisions/2026-09-12-rebalance-starves-health-port.md` (Proposal A,
`eventbus@5c3b965`). That diagnosis was right about the *mechanism* and wrong about the *magnitude*
by a factor of four, and its N=3 threshold could not have worked.

---

## 1. What the cluster actually showed

Verified live, not carried over from the previous doc:

```
$ kubectl get pods -n schematic | grep eventbus
eventbus0-7d58c67b5b-z2mhb   0/1   CrashLoopBackOff   7 (4m16s ago)   35m

$ kubectl describe pod eventbus0-7d58c67b5b-z2mhb -n schematic
    Last State:   Terminated   Reason: Error   Exit Code: 137
    Liveness:     grpc <pod>:8444 delay=10s timeout=5s period=10s failureThreshold=3
    Startup:      grpc <pod>:8444 delay=0s  timeout=5s period=5s  failureThreshold=60
  Warning  Unhealthy  4m52s (x24 over 29m)  Liveness probe failed: service unhealthy (responded with "NOT_SERVING")
  Normal   Killing    4m52s (x8 over 29m)   Container eventbus0 failed liveness probe, will be restarted
```

So: killed by the **kubelet liveness probe**, on a genuine `NOT_SERVING` answer (not a timeout),
exit 137. That confirms the previous doc's §5 measurement question — it is the `setReady(false)`
path, i.e. eventbus's own health check voting itself dead. The launcher's 5m `HEALTH_TIMEOUT` never
came into play; the kubelet's 30s budget got there first.

### The herd is real, and it is handled correctly

25 subscribe sessions arrive in a **730ms** window after restart:

```
19:03:30.230  first  "Subscribed to topic(s)"
19:03:30.958  25th   "Subscribed to topic(s)"
```

They land in only **5 distinct consumer groups**, and the largest holds 16 members, each subscribing
to a *different single topic*:

| consumer group | log lines |
|---|---|
| `spiffe://local.dev/system/services/stratum\|stratum` | 290 |
| `…stratum\|stratum-stratum-{0,1,2}` | 136 |
| `eventbus-trace-dispatch` | 35 |
| `…scottyagent\|scottyagent` | 24 |
| `…mailbox\|mailbox` | 24 |

**This part already works.** `group.initial.rebalance.delay.ms=10000`
(`KafkaEmbeddedBroker.java:318`) batched that whole burst into a single rebalance, exactly as its
comment claims:

```
19:03:36.938  Stabilized group …|stratum generation 373 (__consumer_offsets-0) with 15 members
19:03:37.011  Assignment received from leader …
19:03:37-50   Partition assignment complete: topic=… (x10)
```

15 members, one rebalance, ~6 seconds. **No staggering or jitter layer is needed and none was
added** — see §5.

### The actual engine

The group was stable and serving. Then one late member joined:

```
19:04:07.087  Member …|stratum-17 … joining group
19:04:07.088  Preparing to rebalance group …|stratum in state PreparingRebalance with old generation 373
```

`group.initial.rebalance.delay.ms` only batches the first rebalance of an **empty** group, so this
one starts immediately and the other 15 members must all rejoin before it can complete. Then, 60
seconds after their polls stopped returning, **every waiting session gave up at the same instant**:

```
19:04:35.505  WARN Partition assignment deadline of 60000ms expired on topic services.mailbox…
19:04:35.635  WARN Partition assignment deadline of 60000ms expired on topic …AgentSessionStateBus…
19:04:35.637  WARN Partition assignment deadline of 60000ms expired on topic …AgentCycleBus…
…16 of these inside 460ms…

19:04:48.755  ERROR Consumer poll loop STALLED … no successful poll for 72903ms (max.poll.interval.ms=60000)
…16 of these inside 2ms…
```

Those two numbers were **identical by construction**:

| | value |
|---|---|
| assignment budget = `ASSIGNMENT_MAX_ATTEMPTS(12) × ASSIGNMENT_POLL_MS(5000)` | **60000ms** |
| `MAX_POLL_INTERVAL_MS` → `max.poll.interval.ms` | **60000ms** |

A session waiting for partitions is blocked *inside* `poll()` — the file documents this at
`KafkaSubscriber.java:56` ("`poll(timeout)` does not bound the JoinGroup/SyncGroup handshake"). So
the assignment budget and the not-polling interval are the same stretch of wall clock, measured by
two different parties. When they are equal, at t+60s every member of a still-rebalancing group
simultaneously:

1. **abandons its own assignment wait** → `PartitionAssignmentTimeoutException` → session ends →
   `consumer.close()` leaves the group → *that starts another rebalance*, and
2. **is evicted by the coordinator** for exceeding `max.poll.interval.ms` → *that starts another
   rebalance*.

A rebalance cannot complete until every member rejoins. So a group that needs longer than the budget
to converge **can never converge at all**: the deadline fires, 16 members leave and are evicted
together, each departure re-dirties the group, the clients retry `Subscribe`, new members join, and
the 60s clock restarts. 64 `(Re-)joining group` events for 26 sessions. Session counters at the end:

```
totals[started=26, assigned=11, assign_timeouts=7, failed=0, poll_stalls=20]
```

Only then does the health check lose:

```
19:04:52.547  WARN Kafka health check failed (1/3 consecutive) — still reporting HEALTHY
19:05:02.553  WARN Kafka health check failed (2/3 consecutive) — still reporting HEALTHY
19:05:12.560  WARN Kafka health check failed (3 consecutive) — reporting UNHEALTHY
              eventbus: health: 127.0.0.1:8443 status=NOT_SERVING
19:05:39.588  Shutdown hook triggered          ← kubelet SIGKILL, exit 137
```

**The health check was the messenger, not the cause.** It reported a broker that genuinely could not
answer `describeCluster()`, because its own consumer groups were in a permanent rebalance loop that
eventbus's timeout configuration created.

## 2. Why the existing N=3 fix could not have worked

`eventbus@5c3b965` set `UNHEALTHY_STREAK_THRESHOLD = 3`, reasoned as "~30s of tolerated Kafka
unavailability … matching the usual k8s `failureThreshold: 3`". Two problems:

- **It was sized against the k8s probe default rather than against a measurement.** The real
  post-restart assignment wait was 87-135s (longest observed: 135786ms, a scottyagent
  `AgentCycleBus` session). A 30s budget does not tolerate a normal restart, let alone a bad one, so
  the pod was killed mid-rebalance *every* time — and per the previous doc's own step 6, each kill
  makes the next rebalance dirtier. It never self-stabilized: 7 restarts and still looping.
- **30s internal + 30s kubelet do not add up to 60s of tolerance.** They overlap. Both are driven
  from the same `HealthStatusManager`, so the kubelet's three failures are counted *after* the
  streak flips, and the pod dies ~30s after eventbus first reports UNHEALTHY regardless.

Proposal B from that doc (the watchdog on the `createSession` poll) is live and working — the
`Initial assignment poll of 5000ms exceeded … waking consumer` lines are it doing its job. It is not
implicated.

## 3. What changed

### eventbus

`kafka/KafkaSubscriber.java`

| constant | before | after | why |
|---|---|---|---|
| `ASSIGNMENT_MAX_ATTEMPTS` | 12 (→ 60s budget) | **36** (→ **180s** budget) | must exceed the real 135s worst case, or the timeout fires on a normal restart |
| `MAX_POLL_INTERVAL_MS` | 60000 | **300000** | must exceed the assignment budget, so a session waiting for partitions is never evicted while it waits. 300000 is also Kafka's own default, not an invented number |
| `POLL_STALL_WARN_MS` | *(did not exist)* | **60000** | new; see below |

The three now stand in the only order that is safe: `180s budget < 300s eviction ceiling`, with a
120s gap so a session that burns its *entire* budget still has room to get back into `poll()`.

Raising the eviction ceiling would, on its own, have cost the wedged-consumer detection that the
60000ms value was lowered to provide ("the group reports Stable, the pod is 1/1 Ready, and nothing
is consumed"). So **detection was split from eviction**: the poll-stall watchdog now reports against
the new `POLL_STALL_WARN_MS`, still 60s, so that failure mode is surfaced exactly as fast as before
— it is simply reported by eventbus's own watchdog instead of being *enforced* by the coordinator
dropping the member. The watchdog's scan period follows it (30s) instead of following the eviction
ceiling, which would otherwise have coarsened to 150s.

One consequence of raising the ceiling had to be handled: `PROCESSING_BUDGET_MS`, which drives the
mid-batch keep-alive, was derived as `MAX_POLL_INTERVAL_MS / 2`. Raising the ceiling to 300000 would
have moved the keep-alive from 30s to 150s — i.e. 90s *after* the poll-stall watchdog reports, so
every legitimately slow batch would have logged a spurious `STALLED` error. It is now derived from
`POLL_STALL_WARN_MS / 2`, which keeps it at its original 30s and restores the intended order
`keep-alive (30s) < stall warning (60s) << eviction (300s)`. A test pins that ordering.

The coincidence is now **impossible to reintroduce silently**. `validateTimeoutBudgets()` runs from
the class's static initializer and throws if the budget reaches the ceiling — including via the
`EVENTBUS_SUBSCRIBER_*` env overrides, which is how it would most plausibly come back. It is a
package-private parameterised method rather than an inline block specifically so the relation is
testable against the values that crash-looped production.

`EventBusHandler.java`

| constant | before | after |
|---|---|---|
| `UNHEALTHY_STREAK_THRESHOLD` | 3 (~30s) | **24** (~**240s**) |

At the generated `HealthReporter`'s fixed 10s cadence that is 240s of tolerated Kafka
unavailability. Sized deliberately:

- **Lower bound**: must clear the observed 135s worst case with real margin. 240s is 1.77x.
- **Upper bound**: must stay inside the launcher's 5m `HEALTH_TIMEOUT`, or the launcher kills the
  pod first with a less useful signal. 240s < 300s.
- A post-restart rebalance is functionally a second cold start, so this is the same order of
  magnitude the `startupProbe8443` already allows for one (300s) — the previous doc's Proposal C
  ("startup grace period") was rejected as too narrow, and this is the general form of the same
  intuition: not a window after `initialized`, but a tolerance sized like a cold start.

`HEALTH_REPORT_INTERVAL_MS` was added mirroring the generated
`HealthReporter.REPORT_INTERVAL_MS = 10_000`, used only so the tests can express the threshold as
wall-clock time. That closes **OQ-3** from the previous doc: the generated constant is not
configurable, so rather than making the streak wall-clock-based, `HealthStreakTest` now asserts the
tolerance in milliseconds and will fail if tacplus ever moves the cadence.

### operator (separate repo: `platform/services/operator`)

`pkg/helm/generator.go` — new `schematic.grpcProbeEventbus` helper, selected for eventbus only:

```
periodSeconds: 10    failureThreshold: 30    → 300s liveness budget   (was 10 x 3 = 30s)
```

This is **eventbus-specific on purpose**. Widening the shared `grpcProbe8443` would have relaxed
liveness for every 8443 service to cover a problem only eventbus has (an embedded broker in its own
JVM). It follows the precedent already in that function, which gives stratum alone a longer
*startup* budget for the same kind of reason.

The two tolerances are deliberately ordered **240s internal < 300s kubelet**: eventbus's own verdict
is what normally restarts the pod, and the kubelet stays the backstop for the case where eventbus is
too wedged to answer at all. The operator test asserts both bounds, so neither can drift into the
other.

## 4. The failure mode this must NOT break

A genuinely wedged KRaft controller must still get the pod restarted — that behaviour is
intentional and load-bearing (`EventBusHandler.java`, "in-place recovery without a human wiping the
PVC"). It still happens, just at **~240s instead of ~30s**. That is the explicit trade:

- **Cost**: up to ~4 minutes serving traffic with a possibly-dead embedded broker, instead of ~30
  seconds. Publishes fail and subscribes hang during that window either way; the pod restarting
  sooner did not make them succeed.
- **Benefit**: the pod is no longer killed during the ~135s rebalance that follows *every* restart,
  which is what converted a single slow restart into an unbounded crash loop.

The old 30s value did not buy 30s-to-recovery in practice. It bought a restart *into a dirtier
rebalance*, i.e. negative recovery. Detection of a wedged controller was never the weak link;
distinguishing "wedged" from "rebalancing" was, and 240s is long enough that only one of the two is
still in progress.

## 5. Considered and deliberately not done

**Staggering / jittering reconnecting subscribers.** The brief asked for this and the data says no.
The herd is not the problem: 15 members joined inside 730ms and the broker folded them into *one*
rebalance in ~6s via `group.initial.rebalance.delay.ms=10000`. Adding a client-side join gate would
duplicate a broker-side mechanism that is already working, on the hot path of every subscribe, to
fix something that was not broken. The storm came from the timeout coincidence in §1, and 25
simultaneous joins are in fact the *best* case for the existing batching. Not implemented.

**Per-topic consumer group ids** — the real structural fix, deliberately deferred. 16 members
subscribing to 6 *disjoint* topics in one group is a Kafka anti-pattern: they are coupled into one
another's rebalances for no benefit, since consumer groups only exist to share partitions of the
*same* topic. Deriving the Kafka group as `<caller-namespaced-group>|<topic>` would eliminate the
cross-topic coupling entirely while preserving genuine partition sharing (the four members on
`MessageBus.Stream` would still land together). **Not done here** because it silently resets
committed offsets for every existing group, and with `auto.offset.reset=earliest` that means a
one-time full replay of every topic to every subscriber — during an incident. It needs its own change
with a migration story. See OQ-1.

**Anything about the late-joining member.** `stratum-17` arriving 30s after the group stabilized is
what triggered the fatal rebalance. Kafka has no server-side batching knob for a non-empty group,
and the fix for the coupling it exposes is the per-topic group id above, not a new mechanism.

**§4 smells from the previous doc** (virtual threads pinning carriers on Java 21, no `-Xmx`).
`-Xmx` is now set — `JDK_JAVA_OPTIONS: -Xmx1g -Xms128m` in the pod spec — so that one is resolved.
The virtual-thread pinning is still real and still not on this causal path.

## 6. Verification

**eventbus** — full suite, `mvn -o test`:

```
Event Bus Codegen ... Tests run: 30, Failures: 0, Errors: 0, Skipped: 0
Event Bus Server  ... Tests run: 70, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

100 tests, 0 failures, 0 errors, 0 skipped. New/changed:

- `kafka/SubscriberTimeoutBudgetTest` (**10 tests, new**) — the regression test for this incident.
  Asserts the invariant on shipped values; asserts the 120s recovery gap; asserts the budget covers
  the observed 135786ms rebalance; and feeds the **exact production configuration**
  `(12, 5000, 60000)` to `validateTimeoutBudgets` to confirm it is now rejected, plus equal-at-any-
  scale and over-ceiling cases. Also pins `POLL_STALL_WARN_MS` at 60s so raising the eviction
  ceiling cannot quietly cost the wedged-consumer detection, and that the mid-batch keep-alive
  still fires before a stall is reported (see the `PROCESSING_BUDGET_MS` note below).
- `HealthStreakTest` (**8 tests**, was 5) — rewritten to assert wall-clock tolerance rather than a
  raw count of 3, since the raw count sized against the k8s default is the thing that failed.
  `theWorstObservedRebalanceDoesNotReportUnhealthy` is the direct reproduction: 13 consecutive
  failed probes (135786ms / 10s) must still report HEALTHY — under the old threshold that returned
  UNHEALTHY at probe 3 and the pod died. `wedgedControllerIsStillRestartedInsideTheLauncherTimeout`
  pins the other side of the trade.
- `ConsumerGroupStarvationTest` (2 tests) and the 3 other Kafka integration suites still pass
  against a real embedded broker, so the raised `max.poll.interval.ms` did not regress either known
  starvation mode.

**operator** — `go build ./...` clean, `go vet ./pkg/helm/` clean, `go test ./pkg/helm/` ok.
`TestEventbusGetsALongerLivenessBudget` (new) parses `periodSeconds` and `failureThreshold` out of
the rendered helper and asserts the product clears 270s and exceeds eventbus's own 240s verdict, so
the two tolerances cannot drift into the wrong order.

Two failures in `pkg/bootstrap` — `TestEnsureServiceDelegatesAppServiceGetsBroker` and
`TestDefaultOPAPolicyUsesGeneratedGrantNames` — are **pre-existing and unrelated** (delegate
authorization and OPA grant catalogs). Confirmed by reverting `generator.go` and re-running: byte
-identical failures.

**Not verified live.** These changes are uncommitted and unbuilt into an image, so the fix is not yet
proven against the running cluster.

The pod's state over this investigation is itself evidence for the diagnosis. It went from 7
restarts to **13** while this was being written, then happened to land a window where
`describeCluster()` answered for long enough and came up `1/1 Running`:

```
13:10  eventbus0-…-z2mhb   0/1   CrashLoopBackOff   7 (4m16s ago)   35m
13:45  eventbus0-…-z2mhb   1/1   Running            13 (91s ago)    69m
```

That is precisely the "self-stabilizes by luck" behaviour the previous doc described at its step 7 —
six more restarts, then a coin flip landed right. It is not a fix and it is not evidence the problem
is gone: the timeout coincidence is still in the running image, so the next restart re-enters the
loop. **Do not read the green pod as the incident being resolved.**

The observable success criterion after deploy: `Partition assignment deadline of … expired` should
disappear from the logs entirely on a normal restart (it currently fires on all ~16 sessions of the
large group), and `Kafka health check failed (N/24 consecutive)` should ramp and then *reset* rather
than reach 24.

## 7. Open questions

**OQ-1 — should the Kafka consumer group be per-topic?** (§5) This is the root of the amplification
and the only change that removes it rather than tolerating it. It is a client-visible contract
change with a one-time offset reset and full replay. Needs a decision from whoever owns stratum's
ingestion, plus a migration plan. **Default proceeded on: not changed.**

**OQ-2 — is ~4 minutes the right tolerance for a wedged controller?** (§4) 240s is my call, bounded
below by the 135s measurement and above by the launcher's 300s `HEALTH_TIMEOUT`, which leaves little
room to move. Raising it further requires also raising `HEALTH_TIMEOUT`, which is unset in this repo
and therefore on its default. Whoever owns the availability SLO should confirm. **Default proceeded
on: 240s internal / 300s kubelet.**

**OQ-3 — should the `EVENTBUS_SUBSCRIBER_*` pattern extend to the health threshold?**
`UNHEALTHY_STREAK_THRESHOLD` is a hard constant while the subscriber timeouts are all env-overridable.
Making it an override would let this be tuned on a live pod without a rebuild, which during this
incident would have been useful. Not done — it is beyond the fix, and the guard in
`validateTimeoutBudgets` deliberately does *not* cover the health threshold, so an override there
would be unvalidated. **Default proceeded on: hard constant.**

**OQ-4 — `checkHealth()` can still hold the health-reporter thread for 20s.**
`getClusterHealth()` has two sequential 10s `.get()` deadlines, so the ramp to UNHEALTHY and the
ramp back to SERVING are both slower than the nominal 10s cadence under exactly the conditions that
matter. Carried forward unchanged from the previous doc's OQ-4. It makes the 240s tolerance
effectively "240s or more", which is conservative in the safe direction here, so it is not urgent —
but the recovery flip back to SERVING is delayed by the same amount, which is not.
