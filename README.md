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
just clean  # remove build artifacts
```

## Proto

[`services/eventbus.proto`](../../proto/services/eventbus.proto)
