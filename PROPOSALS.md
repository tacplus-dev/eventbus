# Proposals: eventbus

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **guava 33.7.2 and snappy-java 1.1.10.9** (2026-10-27, #9): the OSV scan run for #9 found advisories
  published on 2026-10-27: guava 33.5.0-android (via grpc-protobuf) has GHSA-xxph-c9ww-hj94 (HIGH,
  deserialization, fixed 33.7.2), and snappy-java 1.1.10.5 (via kafka-clients 3.9.2) has GHSA-gwq9-9ffj-mvrv
  and GHSA-q233-g92q-qxp6 (HIGH) plus GHSA-xqrx-qh46-34m7 (MODERATE), all fixed in 1.1.10.9. Pin both in the
  parent's dependencyManagement, as #5 did for Kafka's transitive dependencies. Each needs its own issue.
- **Drop Javalin from the generated Java libraries** (2026-10-27, #9): `GrpcServer` declares a `Javalin
  httpServer` that it never assigns or starts, and `ServiceImplementation.registerRESTEndpoints` is never
  called. Health runs over gRPC. Javalin brings in about 20 Jetty 12 jars, which nothing here serves, and
  each Jetty advisory means another bump. Removing it is a change in tacplus (the Java generator), after
  which `eventbus-model` could drop the dependency.

## Promoted / declined
