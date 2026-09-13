# Deploying the assignment-budget crash-loop fix: rebuild, rollout, live verification

**Date:** 2026-09-13 (14:08–15:30 MDT)
**Status:** DEPLOYED AND VERIFIED LIVE — eventbus0 stable, 0 restarts, through a real post-restart
rebalance with a 27-subscriber reconnect herd.
**Deploys:** `eventbus@ff7704e` (this repo) and `operator@90dc6d9` (probe helper only — see §5, it
is NOT deployed as the branch tip).
**Companion to:** `decisions/2026-09-13-assignment-budget-equals-poll-interval.md`, which diagnosed
the bug and changed the code. This doc covers only the build/deploy/verify half, which that doc
explicitly left open ("Live pod still crash-looping on old image at commit time").

---

## 1. Before

```
$ kubectl get pods -n schematic | grep eventbus     # 14:09 MDT
eventbus0-7d58c67b5b-z2mhb   1/1   Running   16 (5m42s ago)   95m
```

That same pod, followed to 14:46 MDT, reached **20 restarts in 132 minutes** — roughly one restart
every 6.6 minutes, sustained. It was never in `CrashLoopBackOff` at the moments I sampled it: it
cycled Running → killed → Running, which is what the liveness probe killing a healthy-but-
rebalancing process looks like.

## 2. After

```
$ kubectl get pods -n schematic | grep eventbus     # 15:28 MDT
eventbus0-7d859dbc59-xkkd8   1/1   Running   0   15m
```

**0 restarts across 15+ minutes of continuous polling** (sampled every 30–60s from 15:13:10 to
15:28:50), against a prior rate of one restart per ~6.6 minutes. Over that same window the old pod
would have restarted twice.

Running image digest verified equal to the one I pushed, not a stale cached layer:

```
$ kubectl get pod eventbus0-7d859dbc59-xkkd8 -n schematic -o jsonpath='{.status.containerStatuses[0].imageID}'
us-central1-docker.pkg.dev/.../schematic/eventbus@sha256:0dc7eb727142c74ea076da3ada3f0d18b2053747482cdd0c9926fa4455839589
$ docker inspect --format='{{index .RepoDigests 0}}' .../schematic/eventbus:local
us-central1-docker.pkg.dev/.../schematic/eventbus@sha256:0dc7eb727142c74ea076da3ada3f0d18b2053747482cdd0c9926fa4455839589
```

## 3. It rode out a real rebalance — the fix is doing exactly what it was designed to do

The herd is real and it happened on this pod: **27 `Subscribed to topic(s)` sessions.** The new
budgets are visibly in force in the live logs:

```
Partition assignment deadline of 180000ms expired on topic services.work.work.WorkActivityBus.Stream, waking consumer
Consumer poll loop STALLED on topic ...AccountingRecordBus.Stream: no successful poll for 61911ms
  (warn after 60000ms, coordinator evicts at max.poll.interval.ms=300000).
```

Compare the pre-fix signature from the diagnosis doc:

```
WARN  Partition assignment deadline of 60000ms expired ...
ERROR Consumer poll loop STALLED ... no successful poll for 72903ms (max.poll.interval.ms=60000)
```

Three things changed and all three are confirmed live:
- assignment budget **60000ms → 180000ms**
- coordinator eviction ceiling **60000ms → 300000ms**
- poll-stall detection **decoupled** from the eviction ceiling — it now warns at 60s and says so
  in the message, instead of coinciding with eviction

**Read the STALLED lines correctly.** There are 30 of them on the healthy pod and that is not a
regression — post-fix, `STALLED` is a deliberately non-fatal 60s warning whose own text names a
300s ceiling it never reached. The pre-fix line was fatal because the warn threshold *was* the
eviction ceiling. Grepping for the string alone would give the wrong answer here.

18 assignment-deadline expiries did occur, all inside the first ~2 minutes (21:15:09–21:17:06Z),
and then stopped. So some groups genuinely needed longer than 180s to converge — worth knowing —
but expiry now wakes the consumer to retry rather than colliding with coordinator eviction, and
the pod absorbed it without a restart. **That is the whole point of the fix: the two limits no
longer fire at the same instant.**

## 4. The other two fixes did not regress

- `PERMISSION_DENIED`: **0** occurrences (watch-denial-storm fix, tacplus `b46e8ac1` isStore).
  Pre-fix baseline recorded in the looper journal was ~710 denials / 5 min.
- Per-map durability (stratum `64f04b7`): no related errors; stratum-0/1/2 all Running.
- Fleet: **20/20 Running** at 15:28.

## 5. What it actually took to build this, and what is NOT as-committed

Four things went wrong that are worth recording, because none of them are obvious and three of
them will bite the next person.

**5a. `model/target` had to be regenerated, and the briefing's pipeline was the wrong one.**
The contaminated tree was wiped (`rm -rf model/target`) and regenerated through the real pipeline
mechanism (`tacplus build --tag latest --src <platform> --src <app>`). Critically, **the real
pipeline for this cluster is `scotty/backend/build.sh`, not `testapp/build.sh`.** I built the first
image with testapp's proto set. It deployed, and `stratum-0` immediately started logging
`UNIMPLEMENTED: Method not found: scotty.ScottyActivityBusStateService/SubscribeToStream` — the
image had lost every scotty bus topic. Rebuilt with `--src platform/proto --src
scotty/backend/proto` at scotty's own pin (`b46e8ac1`); UNIMPLEMENTED went to 0 and the herd
reappeared at 27 sessions. The live fleet is the scotty app (mailbox, messaging, scotty,
scottyagent, scottypersonal, weather, work) — testapp's service list does not even contain
mailbox or messaging.

**5b. Builds were done in isolated git worktrees, because another agent was concurrently
clobbering the tree.** Mid-build, a `saml-repin-and-verify` child began regenerating every platform
service against `testapp/proto` and rewrote `eventbus/model/target` underneath a passing test run.
Everything after that point was built in `git worktree` copies under the scratchpad, so the image
corresponds to a known commit and nothing else could race it.

**5c. `testapp/proto`'s working tree blocks eventbus generation outright.** 72 uncommitted lines
add a `DelegationProbe` record whose generated topic declares no `auth_services`; eventbus's codegen
plugin hard-fails on it. Reproduced identically on tacplus `b46e8ac1` and `991b2191`, so it is a
proto gap, not a generator regression. Not mine to fix — left alone.

**5d. The operator branch `d3-stratum-c2s-split` is not deployable as committed.** Two independent
blockers, neither related to the probe change:
  1. `88281b2` emits a NetworkPolicy the operator's ServiceAccount has no RBAC for. Deploying the
     branch tip killed every reconcile with
     `networkpolicies.networking.k8s.io "stratum-hazelcast" is forbidden`.
  2. `platform/proto@7a8a596` made `oauth.ProviderConfig.allowed_redirect_uri_prefixes` required
     with `min_size: 1`, which the live operator config does not populate. An operator built from
     current protos refuses every reconcile with
     `field AllowedRedirectUriPrefixes below min size of 1, got: 0`.

  I hit both live and rolled the operator back within ~4 minutes each time; reconciliation was
  confirmed restored (helm revisions incrementing, `reconcile failed` count 0) before continuing.
  The operator now running is built from `1021978` + a cherry-pick of the probe commit `90dc6d9`
  only, with `model/` generated at `platform/proto@7a8a596^` and `pkg/bootstrap/registry.json`
  carried over unchanged from the previously deployed build. **It therefore corresponds to no
  committed branch tip.** That was a build-input choice, not a source edit — but it is drift, it is
  deliberate, and both blockers above need an owner decision before the branch can ship as-is.
  Written up in the looper's `inbox.md`.

The probe is live and correct on the pod:

```
$ kubectl get deploy eventbus0 -n schematic -o jsonpath='{...livenessProbe}'
{"failureThreshold":30,"grpc":{"port":8444},"initialDelaySeconds":10,"periodSeconds":10,
 "timeoutSeconds":5}
```

300s budget, ordered behind eventbus's own ~240s `UNHEALTHY_STREAK_THRESHOLD`, exactly as
`schematic.grpcProbeEventbus` intends.

**Side effect worth naming:** deploying the operator triggered a fleet-wide reconcile churn —
kernel-0/1/2, kmaker0 and scotty0 each restarted once. All came back healthy. Not requested, not
harmful, but it is what bumping the operator image does here.

## 6. Verification actually performed

- `mvn -pl server -am test` after the clean regen: **70/70 pass** (run three times across the
  three generation variants; the scotty-proto build that was actually shipped passed 70/70).
- operator `go build ./...`, the `validate-generated-output` gate, and `go test ./pkg/helm/...`:
  all pass on the tree that was shipped.
- Image digest on the running pod matched the pushed digest.
- 15+ minutes of live polling at 30–60s intervals, 0 restarts.

## 7. Honest limits

- 15 minutes is enough to clear the pre-fix ~6.6-minute restart cadence twice over, and it covers
  the post-restart rebalance that was the actual trigger — but it is not a long soak. If the
  crash-loop had a slower second mode, this would not have caught it.
- The 18 assignment-deadline expiries mean 180s is not comfortably above the real worst case for
  every group. It did not cause a restart, but the margin is thinner than the diagnosis doc's
  87–135s measurement implies. Worth re-measuring if this recurs.
- The operator half is deployed off a constructed tree (§5d), so "the operator fix is live" is true
  of the probe helper and not true of the branch.
