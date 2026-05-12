# EventBus Release Image
# Layers the event-bus jar and eventbus binary onto a JRE base.

FROM registry.gitlab.com/secondstarcavalier/schematic/containers/jre/schematic-jre-1-3:latest

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

RUN groupadd -r eventbus && useradd -r -g eventbus eventbus

COPY server/target/event-bus.jar /app/event-bus.jar
COPY eventbus /usr/local/bin/eventbus
RUN chmod +x /usr/local/bin/eventbus \
    && mkdir -p /var/lib/eventbus/data \
    && chown -R eventbus:eventbus /var/lib/eventbus

USER eventbus

VOLUME /var/lib/eventbus/data

ENTRYPOINT ["eventbus"]
