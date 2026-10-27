# EventBus Release Image
# Distroless Java 21 base with no shell.
# The launcher binary reads LAUNCH_CMD and supervises the java process.

# Pre-create /var/lib/eventbus with correct ownership for nonroot (65532).
FROM busybox AS setup
RUN mkdir -p /var/lib/eventbus/data

FROM gcr.io/distroless/java21-debian13

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Seabeam LLC <cloudctlplane@seabeam.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Seabeam LLC" \
      org.opencontainers.image.title="EventBus" \
      org.opencontainers.image.description="Event bus service with embedded Kafka broker"

COPY --from=setup --chown=65532:65532 /var/lib/eventbus /var/lib/eventbus
COPY server/target/event-bus.jar /app/event-bus.jar

ARG TARGETARCH
COPY launcher-linux-${TARGETARCH} /usr/local/bin/launcher

USER nonroot

ENV LAUNCH_CMD="java -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -Djava.awt.headless=true -jar /app/event-bus.jar"
ENV HEALTH_PORT=8443

VOLUME /var/lib/eventbus/data

ENTRYPOINT ["launcher"]
