# Proposals: eventbus

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
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
  ports are taken), so its load ran the other classes. `TopicManagerListTopicsTest` (#24) takes 39792/39793.
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
