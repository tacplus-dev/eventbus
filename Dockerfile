# EventBus Release Image
# Layers the event-bus jar and eventbus binary onto a JRE base.

FROM eclipse-temurin:21-jre-alpine

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Second Star Cavalier LLC <cloudctlplane@secondstarcavalier.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Second Star Cavalier LLC" \
      org.opencontainers.image.title="EventBus" \
      org.opencontainers.image.description="Event bus service with embedded Kafka broker"

ARG GRPC_HEALTH_PROBE_VERSION=0.4.48
ARG TARGETARCH
RUN wget -qO /usr/local/bin/grpc_health_probe \
      "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}" \
    && chmod +x /usr/local/bin/grpc_health_probe \
    && addgroup -S eventbus && adduser -S eventbus -G eventbus

COPY server/target/event-bus.jar /app/event-bus.jar
COPY eventbus /usr/local/bin/eventbus
RUN chmod +x /usr/local/bin/eventbus \
    && mkdir -p /var/lib/eventbus/data \
    && chown -R eventbus:eventbus /var/lib/eventbus

USER eventbus

VOLUME /var/lib/eventbus/data

ENTRYPOINT ["eventbus"]
