# EventBus

EventBus is a distributed event streaming service built on embedded Kafka
(KRaft mode, no ZooKeeper). It provides typed pub/sub with replay,
configurable backpressure, consumer group management, and topic administration.
Topics are auto-generated from proto state field annotations — each annotated
field gets typed Publish and Subscribe RPCs.

## Architecture

```
  producers ──► Publish{State}{Field}  ──► embedded Kafka ──► Subscribe{State}{Field} ──► consumers
                                              │
                                        KRaft consensus
                                        (controller + broker)
```

EventBus nodes run in KRaft mode with configurable roles (controller, broker,
or both). The service manages its own Kafka lifecycle, including cluster
formation, topic creation, and partition management. State changes from any
service's proto-defined state fields can be published to and consumed from
EventBus topics.

## RPCs

### eventbus.Admin

| RPC | Description |
|-----|-------------|
| `ListTopics` | List all topics |
| `GetTopicInfo` | Get topic metadata (partitions, config, offsets) |
| `DeleteTopic` | Delete a topic |
| `AlterTopicConfig` | Modify topic configuration |
| `GetConsumerGroupLag` | Get consumer group lag |
| `ListConsumerGroups` | List all consumer groups |
| `ResetConsumerGroupOffsets` | Reset offsets for a consumer group |
| `DeleteConsumerGroup` | Delete a consumer group |
| `GetClusterHealth` | Get embedded Kafka cluster health |

### eventbus.TraceSession

| RPC | Description |
|-----|-------------|
| `WatchTrace` | Server-streaming — watch trace events by trace_id |

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `node_role` | — | KRaft process role (controller, broker, or both) |
| `data_dir` | /var/lib/eventbus/data | Kafka data directory |
| `default_retention_hours` | 168 | Default topic retention (7 days) |
| `default_partitions` | 6 | Default partition count for new topics |
| `default_replication_factor` | 3 | Default replication factor |
| `controller_count` | 3 | Number of controller nodes |
| `broker_count` | 1 | Number of broker nodes |
| `broker_port` | 9092 | Kafka broker listen port |
| `controller_port` | 9093 | Kafka controller listen port |
| `cluster_seed` | — | Cluster seed for node discovery (required) |

## Build

```bash
just        # generate + compile
just gates  # generate, check generated output is untracked, build, test
just clean  # remove build artifacts
```

The top-level justfile is the only build entry point; the modules have no justfiles of their own.

## Releasing

eventbus is released by version, and fleets pin a release line, `MAJOR.MINOR`, which is always that line's
highest patch ([tacplus#180](https://github.com/tacplus-dev/tacplus/issues/180)). Versions are `0.x` until
SeaBeam's launch (2026-12-01), when `1.0.0` is cut; on `0.x` a minor may break, a patch never does.

A change merged to main is released by the same run that merged it: a fix as the next patch, a feature as the
next minor. To cut one, from the green commit to release:

```sh
git tag -a v0.2.0 -m "eventbus 0.2.0: ..."   # vMAJOR.MINOR.PATCH; a patch to an older line is tagged on release/MAJOR.MINOR
git push origin v0.2.0
```

The tag runs [release.yml](.github/workflows/release.yml): `just gates` on the tag in the build image its
`tacplus.lock` pins, then `ghcr.io/tacplus-dev/eventbus:<version>` from the [Dockerfile](Dockerfile), labelled
with the version and commit (`org.opencontainers.image.version`, `.revision`), and `:<MAJOR.MINOR>` moved to it
only when it is the line's highest patch, so the line tag never goes backwards and no other line moves
([tools/release-tags.sh](tools/release-tags.sh)). The GitHub release's notes are the commits since the previous
release ([tools/release-notes.sh](tools/release-notes.sh)); a patch on an older line is never marked Latest. A
pre-release (`v0.2.0-rc.1`) publishes only its own tag. The image is built for linux/arm64; amd64 is off, as in
the fleets, and `PLATFORMS` in release.yml turns it on. It runs under the
[launcher](https://github.com/tacplus-dev/launcher), built from source at the commit `LAUNCHER_COMMIT` names.
The package is public, so pulls need no credential.

Running the workflow by hand is a dry run: `gh workflow run release.yml -f version=v0.2.0` tags HEAD as that
version on the runner, runs the gates, builds the image and prints the tags and notes it would get, and
publishes nothing. `-f existing=true` publishes a tag that already exists (pushed before release.yml, or whose
run failed) from its own tree. A tag without a `tacplus.lock` (v0.1.0) cannot be rebuilt as it was gated, and
is refused: release the next patch instead.

## Proto

[`services/eventbus.proto`](../../proto/services/eventbus.proto)
