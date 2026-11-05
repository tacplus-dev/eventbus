# Proposals: eventbus

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **A security report from the build pipeline, and README's scan sections** (2026-11-05, nightly scan): the scan's
  checklist reads "the build pipeline's security report (README: Security checks)" and "README: Trace propagation".
  README has neither section, and the only pipeline is `.github/workflows/planning-files.yml`, so no report exists to read.
  The scan does the advisory work by hand each night: OSV over the resolved trees, trivy over `event-bus.jar`
  (#21), and the plugin realms (#22, #23). A CI job that publishes those results, with a README section naming it, would
  give the scan, and anyone else, one place to read them. It belongs with the release CI (#2) and the gate decision
  in "More checks in `just gates`" below.
- **Which Java line the base image follows** (2026-11-05, nightly scan): eventbus and stratum run on
  `gcr.io/distroless/java21-debian13` (#11, tacplus-dev/stratum#14 kept the JDK at 21 and moved only Debian). Kernel moved
  to `java25-debian13` under the same rule (tacplus-dev/kernel#17, "behind the latest line"). Which line the rule means,
  Debian only or Java LTS too, is not written down anywhere. A platform decision would settle it for every Java service.
  Moving here would also mean `maven.compiler.release` 25 and a test run on JDK 25.
- **Scan the built jar in the gates** (2026-11-04, #21): the OSV check of the resolved Maven trees reports 0
  advisories, yet `trivy rootfs server/target/event-bus.jar` finds 8 HIGH in jackson 2.21.2 / 3.1.2 shaded inside
  Hazelcast, which no pom tree lists (#21 dropped Hazelcast, which nothing used; the jar now scans clean). stratum added `just audit-jar` to its gates (trivy, HIGH/CRITICAL, exit 1;
  tacplus-dev/stratum#28). The same recipe here would catch shaded copies on every change. Like the OSV gate in
  "More checks in `just gates`" below, it would turn main red when a new advisory is published, so it needs the
  same decision.
- **`looper-trace check` does not read Java** (2026-11-03, #19): it reports "not checked: 41 Java files"
  and passes with 0 handlers, so every eventbus handler's trace propagation (tacplus-dev/looper#51) rests
  on reading it by hand. `PublishHandler` (the only handler whose requests carry field 200) is now covered
  by `PublishTraceIdTest`; any new handler or causal call is not caught by the tool. Java support belongs
  in tacplus-dev/looper, not here.
- **Drop the antrun plexus-utils override** (2026-11-03, #20): `pom.xml`'s `<pluginManagement>` overrides
  maven-antrun-plugin 3.2.0's plexus-utils to 4.0.3 (GHSA-6fmv-xxpf-w3cw; 3.2.0, the newest release, asks
  for 4.0.2), per [fleet-dev docs/java-plugins.md](https://github.com/tacplus-dev/fleet-dev/blob/main/docs/java-plugins.md)
  (tacplus-dev/fleet-dev#18). Remove it once an antrun release asks for 4.0.3 or later and fleet-dev pins it.
- **Fixed Kafka ports in server tests** (2026-11-03, #20): the server tests' embedded brokers bind
  fixed ports, and `KafkaEmbeddedBroker` adds 10000 for the LOCAL listener, so `ReplayIntegrationTest`'s
  LOCAL listener (29092 + 10000) is `ConsumerGroupStarvationTest`'s broker port (39092). It failed with
  `Address already in use` while another worktree's gates ran on this machine. Concurrent looper runs make
  the server tests flaky; ephemeral ports (bind 0, read back the bound port) would let gates run side by side.
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
