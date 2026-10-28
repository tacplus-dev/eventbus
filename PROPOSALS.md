# Proposals: eventbus

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **plexus-utils 3.6.1 in codegen** (2026-10-28, #12): the OSV scan of the resolved tree for #12 reports
  plexus-utils 3.5.1 (provided, via codegen's Maven plugin API dependencies) with GHSA-6fmv-xxpf-w3cw (HIGH,
  directory traversal in `extractFile`, fixed 3.6.1). It is build-time only and not in the server image, so
  the risk is low, but the nightly scan will keep reporting it: pin 3.6.1 in dependencyManagement or bump
  the Maven API dependency that brings it. Needs its own issue.
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
