# Runax.Messaging documentation

Runax.Messaging is a lightweight publish/subscribe library for .NET: a small core of
abstractions with a pluggable transport per broker. As of 2.0, configuration is organized
around **buses** — a bus is a named, self-contained messaging context wrapping exactly one
transport, plus the consumers, serialization, retry policy, and (optionally) outbox around it.

New to the library? Read the [root README](../README.md) quick start first, then
[Buses](buses.md).

## Concepts & guides

| Guide | What it covers |
| --- | --- |
| [Buses](buses.md) | The core model: declaring buses, the one-transport invariant, bus modes (`PublishOnly` / `ConsumeOnly`), resolving buses from DI, multi-bus patterns, lifecycle |
| [Publishing](publishing.md) | `IBus` — the single publishing abstraction: topics, headers, batching, fan-out across buses, the publish pipeline and its telemetry |
| [Consuming & reliability](consuming.md) | Writing consumers, contract versioning, the dispatch pipeline, retries and backoff, poison messages, dead-lettering (framework-managed and broker-native), unroutable messages |
| [Message contracts](contracts.md) | Designing and sharing contract types, `[MessageContract]` versioning, evolution rules, safe rollout with `IMessageContractCatalog` |
| [Transactional outbox](outbox.md) | The dual-write problem, wiring `AddOutbox` + `AddOutboxStore`, how the publish sink and dispatcher work, writing a database-backed store, delivery guarantees |
| [Testing](testing.md) | The broker-free TestKit harness, asserting retries and dead-letters, multi-bus tests, configuration tests, docker-based integration tests |
| [Observability](observability.md) | OpenTelemetry-ready tracing and metrics, the `messaging.runax.bus` tag, per-bus health checks, significant log events |

## Reference

| Page | What it covers |
| --- | --- |
| [Migrating from 1.x to 2.0](migrating-to-2.0.md) | Mechanical migration rules, behavioral changes, wire-format compatibility |
| [Configuration](configuration.md) | Every `BusBuilder` setting, scoping and fallback rules, bus modes reference |
| [Architecture](architecture.md) | Package layering, key types, publish and consume flows, design rules |
| [Serialization](serialization.md) | The `__runax` envelope, body serializers, per-bus and per-topic configuration, AOT/source-gen |
| [Writing a custom transport](writing-a-custom-transport.md) | The `IMessagingTransport` SPI and `TransportConfig` pattern for new brokers |

## Per-package READMEs

Each shipped package documents its own surface: the
[core abstractions](../src/Runax.Messaging.Abstractions/README.md), the
[core implementation](../src/Runax.Messaging/README.md), every transport
(e.g. [RabbitMQ](../src/Runax.Messaging.Transports.RabbitMq/README.md),
[Kafka](../src/Runax.Messaging.Transports.Kafka/README.md),
[Amazon SQS](../src/Runax.Messaging.Transports.Aws.Sqs/README.md)), the
[outbox](../src/Runax.Messaging.Outbox/README.md), and the
[TestKit](../src/Runax.Messaging.TestKit/README.md).
