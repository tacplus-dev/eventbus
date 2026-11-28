# Proposals: eventbus

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **The site plugin's Jetty exceptions expire 2026-12-04** (2026-11-20, #34): `security-exceptions.yaml` accepts
  the maven-site-plugin 3.22.0 realm's Jetty 9.4.58 advisories (GHSA-355h-qmc2-wpwf, GHSA-2fvj-hgj9-j2gr HIGH;
  GHSA-qh8g-58pp-2wxh, GHSA-7p3p-8qv8-m2vh MODERATE) for 14 days, Jody's decision on #34. Every site release,
  4.0.0-M16 too, is on Jetty 9.4 (EOL, no fix on Central). Before then: move the pin to a site release off
  Jetty 9.4 if one ships, else ask for a renewal. Also: pin maven-site-plugin and maven-project-info-reports-plugin
  fleet-wide in tacplus-dev/fleet-dev's `docs/java-plugins.md`, next to fleet-dev#27's default-lifecycle pins;
  every Java service on Maven 3.9.x runs the 3.12.1 default (dom4j 1.1, CRITICAL) and the siblings are unchecked.
- **Embedded Kafka brokers in tests on free ports, not fixed ones** (2026-11-28, #29): each broker test binds
  fixed ports (`TopicManagerListTopicsTest` 39792/39793 and LOCAL 49792, `AdminErrorMappingTest` 39892/39893, …),
  all inside Linux's ephemeral range (32768-60999), and `SubscribeTenantFilterTest` and
  `FleetRecordNamespaceStampTest` share 39692/39693. #29's first local build (6e7a749) failed only at
  `TopicManagerListTopicsTest.startBroker:62` with `BindException: Address already in use` on a looper VM
  that runs other builds at the same time. Picking each port from a bound `ServerSocket(0)` (or letting
  `KafkaEmbeddedBroker` choose and report its ports) would take this flake out of the gates.
- **Generate from a pinned fleet, not the registry's HEAD** (2026-11-22, nightly scan, #37): `tacplus.yaml`'s
  `states: all` (and `depends: kernel`) resolves every service's protos through tacplus-dev/registry when a build
  runs. tacplus.lock pins the compiler but not those inputs. On 2026-11-20 kernel#69 added `TenantRegistryEntry`,
  and main's gates turned red with no eventbus commit (#37). Pinning the fleet's revisions in a lock (written by a
  command like `tacplus pin`, moved by a commit the looper's relock opens, as with the image) would make a fleet
  change arrive as a reviewable PR whose gates run before it lands, per
  tacplus-dev/roadmap decisions/deterministic-builds-2026-10-27.md. This is tacplus behaviour, so it would be a
  tacplus-dev/tacplus feature that eventbus adopts.
- **Pin the Dockerfile's base images by digest** (2026-11-15, #14): the launcher is now a pinned
  tacplus-dev/launcher release checked by sha256, but `gcr.io/distroless/java21-debian13`, `busybox` and the
  fetch stage's `alpine:3.24` are still tags that move (decision deterministic-builds-2026-10-27). Pinning them
  `@sha256:` makes the image reproducible; a bump then is a reviewed change, like `tacplus.lock`. Kernel's and
  stratum's Dockerfiles are the same, and they still copy the launcher from the build context. The release image's
  base is now #38 (and #31); the fetch stage's `alpine:3.24` should go with it.
- **ROADMAP "Now": the floor in force** (2026-11-07, #27): Jody asked on #27 that "Now" name the floor in force,
  but a looper's commit that touches ROADMAP.md is refused. For an agent at Jody's request, "Unreleased; tacplus
  floor 2.4." becomes "Released v0.1.1; tacplus floor 2.6 (2.6.1 pinned in tacplus.lock)." Kernel's is the same
  (tacplus-dev/kernel#46).
- **Drop the plexus-archiver overrides** (2026-11-04, #23): `pom.xml`'s `<pluginManagement>` overrides
  plexus-archiver to 4.15.0 under maven-jar-plugin 3.5.0 (asks for 4.10.4) and maven-plugin-plugin 3.15.1
  (4.10.0), whose older plexus-archiver loads aircompressor 0.27 (GHSA-vx9q-rhv9-3jvg, HIGH; the build, not the
  runtime), per [fleet-dev docs/java-plugins.md](https://github.com/tacplus-dev/fleet-dev/blob/main/docs/java-plugins.md)
  and tacplus-dev/kernel#37. Drop each once a release of that plugin asks for plexus-archiver 4.15.0 or later
  and fleet-dev pins it; the plugins' newer lines are 4.0.0 betas today.
- **A security report from the build pipeline, and README's scan sections** (2026-11-05, nightly scan): the scan's
  checklist reads "the build pipeline's security report (README: Security checks)" and "README: Trace propagation".
  README has neither section, and the only pipeline is `.github/workflows/planning-files.yml`, so no report exists to read.
  The scan does the advisory work by hand each night: OSV over the resolved trees, trivy over `event-bus.jar`
  (#21), and the plugin realms (#22, #23). A CI job that publishes those results, with a README section naming it, would
  give the scan, and anyone else, one place to read them. It belongs with the release CI (#2) and the gate decision
  in "More checks in `just gates`" below.
  Still true on 2026-11-06. That night's scan found guava, jsoup and commons-lang3 advisories in the
  maven-plugin-plugin and os-maven-plugin realms (#26), weeks after some were published. A realm check by hand
  only covers the plugins someone thinks to look at. A job that runs `mvn -X` and queries OSV for every
  `Included:` jar would cover them all.
  Still true on 2026-11-11: README still has neither section, `planning-files` is still the only workflow, and
  code-scanning and Dependabot alerts answer 403 to the scan's token. By hand, the resolved trees and the 113
  plugin and extension realm jars (`mvn -X install`) report no OSV advisories, and `event-bus.jar` and
  `gcr.io/distroless/java21-debian13` scan clean under trivy.
  Still true on 2026-11-12. The scan's runner had no Docker daemon (`failed to connect to the docker API at
  unix:///var/run/docker.sock`), so neither `tools/tacplus.sh` nor `just gates` could run, and there was no
  `event-bus.jar` to scan. By hand: the resolved trees (148 packages), the 113 plugin and extension realm jars, and
  trivy over the 130 runtime jars that go into `event-bus.jar` all report no advisories. trivy now reports 8 HIGH in
  `gcr.io/distroless/java21-debian13` (debian 13.7, `sha256:ff4749f6…`), all `affected` with no Debian fix yet:
  libexpat1 2.9.0-1~deb13u1 (CVE-2026-113351, -115506, -117610, -118370, DoS) and libuuid1 2.41.5-0+deb13u1
  (CVE-2026-76642, -78408, -78409, -78410, mount/nsenter, which a distroless image without those tools does not run).
  No bump can fix them. The Dockerfile follows the tag, so the next image build picks up Debian's fix. A pipeline
  report would show when that lands.
  2026-11-13: the build pipeline now writes a report (looper-scan, `security/summary.md` and `findings.json`), but
  it doesn't cover what the scan checks by hand yet. Tonight's report says "0 finding(s)", but it is for 6bfca94, the
  unmerged `looper/eventbus-28` branch, not main (7e7b541). It also lists under "Not analyzed": osv-scanner "transitive
  dependencies not resolved: failed resolution for .../server/pom.xml: ... eventbus-server[Concrete:1.2.0-SNAPSHOT]"
  (the reactor's own `eventbus-model` SNAPSHOT is in no repository), and spotbugs "the gates compiled no
  target/classes". So the "0" covers none of the Maven tree. The report doesn't look at plugin realms (#18, #20, #22, #23, #26) or
  shaded jars (#21) either. Resolving the server tree needs `eventbus-model` installed first (or a scan after
  `mvn install`), which is a change to looper-scan in tacplus-dev/looper, not here. README still has no "Security
  checks" or "Trace propagation" section. By hand on main: the 149 resolved packages, the 128 plugin and extension
  realm jars (`mvn -X install`, plus exec-maven-plugin 3.6.4's realm) and trivy over the 126 runtime jars report no
  advisories. The base image (`sha256:132eb554…`, still debian 13.7) has the same 8 HIGH, still with no Debian fix.
  The runner still had no Docker daemon, so `just gates` could not run on main (`generate` fails at `docker run`).
  The pinned build image digest still resolves on ghcr.io.
  2026-11-15: the report the scan is handed is again a branch's, not main's: 2e877a5 (`looper/eventbus-2`, the gates
  full on base 988f82e = main), 268 findings (1 HIGH, 135 MEDIUM, 132 LOW, all spotbugs), 0 new, 1 failing the policy,
  the generated `CollectionRuntime.java:102` USO finding #29 already holds. looper-scan publishes every build's
  findings as "the repository's latest", so a branch build's report is what the next nightly reads and what `new`
  is counted against; a nightly of main would read main's. The server tree is still "not analyzed": looper-scan
  knows the case (its `tool_osv` falls back to `--no-resolve` on "a Maven module's sibling SNAPSHOT") and scans
  `server/pom.xml`'s declared dependencies only, so kafka-clients' and the others' transitives are outside the
  floor every build. A fix in tacplus-dev/looper: resolve against the gates' local repository
  (`--experimental-resolution-data-source=native` after `mvn install`) or scan a CycloneDX SBOM the gates put in
  `built/`. By hand on main tonight: the 136 resolved packages (`mvn dependency:tree`, all scopes) and the 112
  plugin and extension realm jars of 18 realms (`mvn -X install -DskipTests -Dtacplus.bin=/bin/true`, which loads
  every realm but server's without tacplus; exec-maven-plugin 3.6.4's from `dependency:resolve-plugins`) report no
  OSV advisories, and trivy agrees on the SBOM. The base image (`sha256:132eb554…`, debian 13.7) has the same 8
  HIGH, still with no Debian fix. No Docker daemon again, so `just gates` could not run here; the build pipeline
  ran `just gates` green on 2e877a5 (base 988f82e; #2's comment) and on 9893207 (#28's land on 988f82e; #29's
  body): main's gates pass, the one red is the security job (#29). tacplus 2.6 is still the latest line (v2.6.12
  the newest patch; the lock's move is #29's).
  2026-11-16: the report is a branch's again, 198fa4d (`looper/eventbus-14`; it differs from main 96e1d66 only in
  Dockerfile, .gitignore and the planning files). It has 268 findings, 0 new, and 1 failing the policy, the same
  `CollectionRuntime.java:102` USO that #29 holds (now waiting on tacplus-dev/tacplus#720; v2.6.13 is the newest
  2.6 patch and lacks the fix). The server tree is still "not analyzed". By hand on main: the 152 resolved
  packages of the four modules, the 113 jars the plugin and extension realms load (`mvn -X install`, as above)
  and exec-maven-plugin 3.6.4's resolved jars report no OSV advisories. trivy finds no HIGH or CRITICAL in the
  129 runtime jars of server. The base image (`sha256:132eb554…`, debian 13.7) has the same 8 HIGH, still with no
  Debian fix. There was again no Docker daemon behind the broker, so `just gates` failed at `generate` and could not
  run here. No code has changed on main since #26 (2026-11-08). `alpine:3.22` in #14's new fetch stage is behind
  the latest line, 3.24 (comment on #14).
  2026-11-18: the nightly report is main's this time (b8f67e3). It has 357 findings, 1 failing the policy (#29's USO)
  and 89 new, all trivy findings on the base image and the Dockerfile. The 79 MEDIUM/LOW/UNKNOWN and DS-0001/DS-0026
  are #31's. The 8 HIGH, still with no fixed package, are the ones #30 closed. They count as `new` only because
  #31's baseline and digest pin haven't landed (both wait on #29). A new problem fails the run: CodeQL's Java
  analysis ran out of memory (#33, needs-decision, with stratum#43, llmproxy#69 and mailbox#21; no
  tacplus-dev/looper issue owns it yet). The server tree is still "not analyzed" by osv-scanner. By hand:
  `mvn dependency:tree` of the four modules (148 packages, all scopes) reports no advisories under osv-scanner
  or trivy. `dependency:resolve-plugins` realms, with the pom's overrides, leave only #32's clean/deploy and
  `maven-site-plugin` 3.12.1, the site lifecycle's default. That one loads jetty 9.4, dom4j 1.1, velocity 1.7,
  maven-core 3.2.5 and others with HIGH/CRITICAL advisories, and even 3.21.0, its newest release, loads jetty 9.4.56,
  commons-io 2.11.0, beanutils 1.9.4 and plexus-utils 4.0.1. No recipe runs `site`, so I filed nothing for it.
  A fleet-dev pin with overrides would close it if anyone ever runs `mvn site`. The gates passed on b8f67e3
  (pipeline 3082135383). tacplus 2.6 is still the latest line (v2.6.13), and distroless publishes no debian14 base.
  2026-11-19: the report is again main's b8f67e3 (main is 1449e39, which changes only PROPOSALS.md), with the same
  357 findings and the same CodeQL out-of-memory problem (#33 closed; tacplus-dev/looper#189 holds it). The server
  tree is still "not analyzed". By hand: `mvn dependency:list` of the four modules (137 packages, all scopes, after
  installing the parent and model POMs into a scratch repository) and the `mvn -X <plugin>:help` realms of the 11
  plugins the build runs (105 jars, overrides applied) report no OSV advisories. `maven-site-plugin` is filed this
  time (#34, needs-decision), on #32's grounds: a lifecycle no recipe runs still loads when anyone runs it. Its real
  `-X` realm (3.12.1) has 1 CRITICAL and 17 HIGH. The newest 3.x is 3.22.0, not 3.21.0. With beanutils 1.11.0 and
  jsoup 1.24.1 overrides, it leaves only Jetty 9.4.58, which has no newer 9.4 on Central. A realm job, as proposed
  above, should load every lifecycle's plugins (site and clean/deploy too), not only `install`'s. The
  `java21-debian13` tag now points to `sha256:3982fcc5…`, not the `132eb554…` that #30/#31's unlanded branches
  pin. trivy shows the same 8 HIGH there, still unfixed.
  2026-11-21: the report is a branch's again: 2271b32 (`looper/eventbus-34`, base 070c950 = main). It has 268
  SpotBugs findings, 1 failing the policy (#29's USO, now waiting on tacplus-dev/tacplus#720). Its summary says
  "268 new" because "the base 070c950acaab (its build published none)". These are the same 268 the 11-15 and 11-16
  reports counted with 0 new, so `new` is measured against whichever build last published a report, not against main.
  With no `security-baseline.json` here, the 267 non-blocking findings were filed as one issue (#35, needs-decision:
  18 hand-written, 249 generated, the latter tacplus-dev/tacplus#854's backlog). Every finding is `"test": false`
  and the summary says "0 SAST in test code", yet 20 are in generated `…Test.java` (e.g.
  `grpcserver/AuthorizerBehaviorTest.java`). looper-scan should classify generated tests as test code, which is a
  change in tacplus-dev/looper. By hand: `mvn dependency:list` (137 packages, all scopes, after `mvn install` of the
  parent and model with `-Dtacplus.bin=/bin/true`) and the 113 jars of the `-X` plugin and extension realms report
  no OSV advisories. trivy finds no HIGH or CRITICAL in the 126 runtime jars of server. `dependency:resolve-plugins`
  (overrides not applied) leaves only #32's clean/deploy and #34's site realm. The gates passed on ebe81b0
  (pipeline 3092282353; main is 070c950, PROPOSALS.md only since). tacplus 2.6 is still the latest line (v2.6.13).
  2026-11-25: the report is not main's again. It covers 13e4186, a land's build started 2026-11-24T18:44:17Z with
  base c553cda, which is main. It has 0 findings and an empty `unfiled.json`, and the server tree is still "not
  analyzed" (the sibling SNAPSHOT). By hand on main: `mvn dependency:tree` run from the reactor root (collection
  only, so no `mvn install` is needed) resolves 136 packages across all scopes, and none has an OSV advisory. The
  nightly gates fail on c553cda (pipeline 3111156258): `sameRpcNameOnTwoTopics_policiesStayApart:357`, which #37
  already holds, so I filed nothing new. tacplus 2.6 is still the latest line (v2.6.14), and the relock is #36's.
  `looper-trace` read none of the 42 Java files. By hand: Publish carries `trace_id` into the Kafka record header
  (`PublishHandler.java:309`, asserted by `PublishTraceIdTest`) and to `dispatchTrace`, whose missing test #28 holds.
  The only kernel call (`discoverVotersFromState`) runs at startup and no request causes it.
  2026-11-26: there was no report for tonight's nightly of 68f7fd2 (pipeline 3114118688, job 18362961559). The
  security job left no `security/` artifact, so its findings are still those of the 13e4186 land build (0 findings,
  `unfiled.json` empty). No finding is in a CI tool either: this repository has no `ci/` tool script. By hand on
  main: `mvn dependency:tree` from the reactor root resolves 148 packages, and none has an OSV advisory (`api.osv.dev`
  querybatch). There is no `security-exceptions.yaml`. The nightly gates fail on 68f7fd2 at
  `sameRpcNameOnTwoTopics_policiesStayApart:357`, which #37 holds. tacplus 2.6 is still the latest line (v2.6.14),
  and the relock is #36's. v2.6.14 carries tacplus-dev/tacplus#720, #29's fix, so I commented on #29. server/ and
  codegen/ have not changed since c553cda, so the trace reading holds: #28 is the one gap. I filed nothing new.
- **Which Java line the base image follows** (2026-11-05, nightly scan): eventbus and stratum run on
  `gcr.io/distroless/java21-debian13` (#11, tacplus-dev/stratum#14 kept the JDK at 21 and moved only Debian). Kernel moved
  to `java25-debian13` under the same rule (tacplus-dev/kernel#17, "behind the latest line"). Which line the rule means,
  Debian only or Java LTS too, is not written down anywhere. A platform decision would settle it for every Java service.
  Moving here would also mean `maven.compiler.release` 25 and a test run on JDK 25.
- **Drop the default-lifecycle and protobuf plexus-utils overrides** (2026-11-04, #22): `pom.xml`'s
  `<pluginManagement>` overrides plexus-utils (GHSA-6fmv-xxpf-w3cw) to 4.0.3 under maven-jar-plugin 3.5.0
  (asks for 4.0.2), maven-resources-plugin 3.4.0 (4.0.2), maven-install-plugin 3.1.4 (4.0.1) and
  maven-plugin-plugin 3.15.1 (4.0.1), and to 3.6.1 under protobuf-maven-plugin 0.6.1 and os-maven-plugin 1.7.1
  (3.1.0; both newest releases), per [fleet-dev docs/java-plugins.md](https://github.com/tacplus-dev/fleet-dev/blob/main/docs/java-plugins.md).
  Drop each once a release of that plugin asks for a fixed plexus-utils (4.0.3+ on the 4.x line, 3.6.1+ on 3.x)
  and fleet-dev pins it; the newer jar, resources, install and plugin-plugin lines are 4.0.0 betas today.
  os-maven-plugin moved from a build `<extension>` to a plugin with `<extensions>true</extensions>` in
  `model/pom.xml` only so it can take the override; it can go back once the override comes out.
  maven-compiler-plugin 3.16.0 (the fleet pin) needs no override. fleet-dev docs/java-plugins.md does not yet
  name maven-install-plugin: worth adding there.
- **Drop the plugin-plugin guava/jsoup and os-maven-plugin commons-lang3 overrides** (2026-11-06, #26): `pom.xml`'s
  `<pluginManagement>` also overrides, under maven-plugin-plugin 3.15.1, guava 33.2.1-jre (GHSA-xxph-c9ww-hj94) to
  33.8.0-jre and jsoup 1.18.1 (GHSA-65r4-943x-97jj, GHSA-pmhh-3w7g-xqp8) to 1.24.1, and under os-maven-plugin 1.7.1
  commons-lang3 3.5 (GHSA-j288-q9x7-2f5v) to 3.21.0. Drop each once a release of that plugin asks for a fixed
  version (guava 33.7.2+, jsoup 1.23.2+, commons-lang3 3.18.0+) and fleet-dev pins it; plugin-plugin's newer line is
  4.0.0-beta-2 and 1.7.1 is os-maven-plugin's newest release. The commons-lang3 override is one more reason
  os-maven-plugin stays a plugin with extensions in `model/pom.xml`. fleet-dev docs/java-plugins.md does not name
  these jars yet: worth adding there. The nightly scan found them only because it queries every plugin realm's
  `Included:` jar from `mvn -X`; the gates' OSV check sees the dependency trees only.
- **Scan the built jar in the gates** (2026-11-04, #21): the OSV check of the resolved Maven trees reports 0
  advisories, yet `trivy rootfs server/target/event-bus.jar` finds 8 HIGH in jackson 2.21.2 / 3.1.2 shaded inside
  Hazelcast, which no pom tree lists (#21 dropped Hazelcast, which nothing used; the jar now scans clean). stratum added `just audit-jar` to its gates (trivy, HIGH/CRITICAL, exit 1;
  tacplus-dev/stratum#28). The same recipe here would catch shaded copies on every change. Like the OSV gate in
  "More checks in `just gates`" below, it would turn main red when a new advisory is published, so it needs the
  same decision.
- **`looper-trace check` does not read Java** (2026-11-03, #19): it reports "not checked: 41 Java files"
  and passes with 0 handlers, so every eventbus handler's trace propagation (tacplus-dev/looper#51) rests
  on reading it by hand. Every request carries field 200, but `PublishHandler`'s Kafka produce is the only
  request-caused call that can carry it (#25); it is covered by `PublishTraceIdTest`, and the generator's
  wiring of it into the `PublishTo*` bindings by `TypedRpcGeneratorTest`. Any new handler or causal call
  is not caught by the tool. Java support belongs
  in tacplus-dev/looper, not here.
  Still true on 2026-11-11: "not checked: 42 Java files", verdict pass (0 handlers). Read by hand, the Admin
  and Subscribe handlers' only request-caused calls are Kafka AdminClient requests and consumer fetches, which
  have no field that could carry `trace_id`. The kernel state calls (`setNodes`, `getAllNodes`, `getConfig`) are
  made at startup and on the refresh timer, not because of a request.
  Corrected on 2026-11-12: the Kafka produce is not the only request-caused call. A traced Publish also sends a
  `TraceEvent` to that trace_id's WatchTrace stream (`EventBusHandler.dispatchTrace` → `WatchTraceHandler.dispatch`),
  and no test covers that hop (#28). A tool that read Java would have listed it.
  Still true on 2026-11-13: "not checked: 42 Java files", verdict pass (0 handlers). Read by hand on main, nothing is
  new beyond #28 (open; its test is on `looper/eventbus-28`, which has not merged).
  Still true on 2026-11-15: "not checked: 42 Java files", verdict pass (0 handlers). Read by hand on main: the
  request-caused calls are PublishHandler.onNext's Kafka produce (the record's `trace_id` header, PublishTraceIdTest)
  and its `dispatchTrace` → `WatchTraceHandler.dispatch` (no test on main; #28, now Blocked-by #29, its test on
  `looper/eventbus-28` at 2816e6a). SubscribeRequest and the Admin requests carry no field 200, and their Kafka
  consumer and AdminClient calls have nothing to carry one; kernel's `getConfig` runs at startup. Nothing to file.
  Still true on 2026-11-16: "not checked: 42 Java files", verdict pass (0 handlers). server/ and codegen/ have not
  changed since then, so the reading holds: #28 is the one gap. Nothing to file.
  Still true on 2026-11-18 (b8f67e3): "not checked: 42 Java files", verdict pass (0 handlers). Read by hand: the
  EventbusState and EventbusConfigState handlers are stubs with no downstream call. The Admin RPCs call only
  `TopicManager` (Kafka AdminClient). Publish's two causal calls are the record header (PublishTraceIdTest) and
  `dispatchTrace`, which still has no test on main (#28). Nothing new to file.
  Still true on 2026-11-20 (ebe81b0): "not checked: 42 Java files", verdict pass (0 handlers). Only PROPOSALS.md
  has changed since b8f67e3, so the 2026-11-18 reading holds: #28 is still the one gap. Nothing to file.
  Covered since #28 by `WatchTraceDispatchTest` (a real `PublishHandler` through `EventBusHandler.dispatchTrace`
  into a real `WatchTraceHandler`).
- **Drop the antrun plexus-utils override** (2026-11-03, #20): `pom.xml`'s `<pluginManagement>` overrides
  maven-antrun-plugin 3.2.0's plexus-utils to 4.0.3 (GHSA-6fmv-xxpf-w3cw; 3.2.0, the newest release, asks
  for 4.0.2), per [fleet-dev docs/java-plugins.md](https://github.com/tacplus-dev/fleet-dev/blob/main/docs/java-plugins.md)
  (tacplus-dev/fleet-dev#18). Remove it once an antrun release asks for 4.0.3 or later and fleet-dev pins it.
- **Fixed Kafka ports in server tests** (2026-11-03, #20): the server tests' embedded brokers bind
  fixed ports, and `KafkaEmbeddedBroker` adds 10000 for the LOCAL listener, so `ReplayIntegrationTest`'s
  LOCAL listener (29092 + 10000) is `ConsumerGroupStarvationTest`'s broker port (39092). It failed with
  `Address already in use` while another worktree's gates ran on this machine. Concurrent looper runs make
  the server tests flaky; ephemeral ports (bind 0, read back the bound port) would let gates run side by side.
  #24 met the same limit: a second `mvn -pl server test` beside `KafkaIntegrationTest` cannot run that class (its
  ports are taken), so its load ran the other classes. `TopicManagerListTopicsTest` (#24) takes 39792/39793, `WatchTraceDispatchTest` (#28) 39992/39993.
- **Drop the shade plexus-utils override** (2026-11-02, #18): `pom.xml`'s `<pluginManagement>` overrides
  maven-shade-plugin's plexus-utils to 3.6.1 (GHSA-6fmv-xxpf-w3cw; shade 3.6.1 asks for 3.5.1, 3.6.2 for
  3.6.0), per [fleet-dev docs/java-plugins.md](https://github.com/tacplus-dev/fleet-dev/blob/main/docs/java-plugins.md)
  (tacplus-dev/fleet-dev#17). Remove it once a shade release asks for plexus-utils 3.6.1 or later and
  fleet-dev pins it. The OSV scan of the resolved trees does not see plugin realms: `mvn -X package`
  (`Populating class realm plugin>...maven-shade-plugin`) does.
- **Stale tacplus image in `server/justfile` and `codegen/justfile`** (2026-10-29, #15): both still run
  Maven inside `ghcr.io/tacplus-dev/tacplus/tacplus:2.0.2`, while the floor is now 2.5 and the top-level
  `just build`/`just gates` build on the host. Either point them at the floor line (or the `tacplus`
  wrapper's image) or drop them in favour of the top-level recipes, so there is one way to build.
  **Fixed** 2026-10-31 (#17): both are deleted; the top-level justfile is the only build entry point.
- **plexus-utils 3.6.1 in codegen** (2026-10-28, #12): the OSV scan of the resolved tree for #12 reports
  plexus-utils 3.5.1 (provided, via codegen's Maven plugin API dependencies) with GHSA-6fmv-xxpf-w3cw (HIGH,
  directory traversal in `extractFile`, fixed 3.6.1). It is build-time only and not in the server image, so
  the risk is low, but the nightly scan will keep reporting it: pin 3.6.1 in dependencyManagement or bump
  the Maven API dependency that brings it. Needs its own issue.
  **Fixed** 2026-10-29 (#16): codegen's `maven-plugin-api` is bumped to 3.9.16, which brings plexus-utils
  3.6.1; the OSV scan of every module's resolved tree reports no advisories.
- **Drop Javalin from the generated Java libraries** (2026-10-27, #9): `GrpcServer` declares a `Javalin
  httpServer` that it never assigns or starts, and `ServiceImplementation.registerRESTEndpoints` is never
  called. Health runs over gRPC. Javalin brings in about 20 Jetty 12 jars, which nothing here serves, and
  each Jetty advisory means another bump. Removing it is a change in tacplus (the Java generator), after
  which `eventbus-model` could drop the dependency.
- **More checks in `just gates`** (2026-10-27, #10): the gates are generate (leaving tracked files as they
  were), check-generated, build and the tests. #10 asked whether a dependency advisory scan (OSV, as run
  by hand for #9) and a format/lint check should also be gates. The tag-only `.gitlab-ci.yml` it cites is
  gone; the release CI is #2. An OSV gate would fail main whenever an advisory is published, so it needs
  a decision on whether a new advisory should block unrelated changes.
- **One SLF4J provider in the release jar** (2026-10-27, #11): `model/pom.xml` and `server/pom.xml` both
  declare `slf4j-simple` 2.0.17 (compile scope) next to logback, so every start prints "Class path contains
  multiple SLF4J providers". SLF4J picks logback today, but which provider wins depends on classpath order.
  Drop `slf4j-simple` (or make it test scope) if nothing needs it. For model's dependency, check first
  whether the tacplus Java generator puts it there.
- **A container smoke check in the gates or CI** (2026-10-27, #11): for #11 the image was checked by hand on
  both architectures (`docker buildx` with a real `event-bus.jar` and a launcher built from
  tacplus-dev/launcher, then a gRPC health check on 8444). It got no further than NOT_SERVING because
  ServiceManager needs a kernel endpoint, and no kernel image is published to run against. A scripted
  check, and a kernel (or stub) to bootstrap against, would let it reach SERVING and check TLS 8443. It
  belongs with the release CI (#2).

## Promoted / declined
