# Changelog

All notable changes to Runax.Messaging are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
All packages in the repository are versioned together.

## [2.0.0] - Unreleased

The bus release. Configuration is restructured around **buses** — a bus is a named,
self-contained messaging context wrapping **exactly one transport**; talking to several brokers
means registering several buses. Migration guide:
[docs/migrating-to-2.0.md](docs/migrating-to-2.0.md). The wire format (`__runax` envelope, DLQ
headers, retry semantics) is unchanged — 1.x and 2.0 services interoperate on the same broker.

### Added

- **Buses** — `AddBus(...)` / `AddBus("name", ...)` with keyed DI resolution
  (`[FromKeyedServices("name")] IBus`), `IBusProvider` for dynamic lookup, and unkeyed `IBus`
  resolving the default bus. Multiple buses may use the same broker type — two RabbitMQ
  clusters are now just two buses.
- **Uniform transport registration** — `bus.AddTransport(config)` with instance, delegate, and
  `IConfiguration`-binding overloads; transport packages ship a `<Broker>Config :
  TransportConfig` type instead of `Add<Broker>` extension methods. Configs are
  DataAnnotations-validated at configuration time.
- **Bus modes** — `bus.Mode = BusMode.PublishOnly / ConsumeOnly`: consumer registrations on a
  publish-only bus and publishes on a consume-only bus throw; a publish-only bus starts no
  consumer hosted service; transports can skip building the unused side via
  `TransportContext.Mode`.
- **Per-bus health checks** — auto-registered as `runax:{bus}` (opt out with
  `RegisterHealthCheck = false`), replacing the manual `Add<Broker>Transport` extensions.
- **Outbox store configs** — `bus.AddOutboxStore(new InMemoryOutboxStoreConfig())` mirrors
  `AddTransport`; store packages derive `OutboxStoreConfig`. `AddOutbox`/`AddOutboxStore`
  pairing is validated at configuration time.
- **Multi-bus TestKit** — `MessagingTestHarnessBuilder.WithBus(...)`,
  `harness.PublishOnBusAsync(...)`, and `RecordedMessage.Bus`.
- **Telemetry** — a `messaging.runax.bus` tag on every span and metric.

### Changed (breaking)

- `IMessagePublisher` and `IMessagePublisherFactory` are deleted; **`IBus` is the only
  publishing abstraction** (identical publish signatures — migrating a publishing class is a
  type swap).
- Per-broker `Add<Broker>` configurator extensions, `TransportBuilder`, `PublishTo`, and
  consumer-to-transport binding by system name are deleted; everything is configured per bus.
- `<Broker>Options` classes are renamed `<Broker>Config` and derive `TransportConfig`.
- Retry, serialization, and unroutable-message scoping is normalized to bus → topic (the 1.x
  global and per-transport scopes merge into the bus scope).
- The outbox routes publishes through the bus's publish pipeline (no more last-wins
  `IMessagePublisher` interception); `IOutboxStore.GetPendingAsync` takes the bus name and
  `OutboxMessage` gains a required `Bus` field; `AddOutbox`/`AddInMemoryOutboxStore` move from
  the root configurator to the bus.
- `IMessagingTransport.SystemName` is descriptive (telemetry) only and no longer required to be
  unique.

## [1.0.0] - 2026-07-26

First stable release.

### Added

- **Core** — a broker-agnostic publish/subscribe API: `IMessagePublisher`,
  `MessageConsumer<T>`, `MessageContext`, and the `IMessagingTransport` SPI, split
  across `Runax.Messaging.Abstractions` (contracts) and `Runax.Messaging`
  (implementation: DI wiring, hosted consumers, JSON serialization).
- **Transports** — publish/consume implementations for RabbitMQ, Apache Kafka,
  Amazon SQS, Amazon SNS (publish to SNS, consume via SQS), Azure Service Bus,
  Azure Event Hubs, Google Cloud Pub/Sub, and Redis Streams (Redis/Valkey), plus a
  built-in in-memory transport.
- **Multiple transports at once** — register several brokers side by side; a
  consumer can bind to one broker or to all, and `PublishTo("<system-name>")`
  selects the default publish target. Inject `IMessagePublisherFactory` and call
  `ForTransport("<system-name>")` to publish the same event to several transports
  explicitly (e.g. both Kafka and SQS).
- **Reliability** — retry with exponential backoff, poison-message handling, and
  framework-managed or broker-native dead-lettering, configurable globally,
  per broker, or per topic via `WithRetry(...)` / `WithRetryForTopic(...)` and
  `OnUnroutableMessage(...)`.
- **Contract versioning** — optional `[MessageContract(version)]`; consumers
  subscribe per version, with a pluggable strategy (dead-letter/requeue/custom)
  for versions no consumer handles.
- **Serialization** — validated options with `IConfiguration` binding, and a
  pluggable body serializer set globally, per broker, or per topic
  (`UseSerializer<T>()` / `UseSerializerForTopic<T>()` and the matching
  `ConfigureSerialization` / `ConfigureSerializationForTopic` options); the
  `__runax` envelope stays framework-owned.
- **Throughput** — batch publish (`PublishBatchAsync`) and concurrent SQS
  consumption (`MaxConcurrentMessages`).
- **Observability** — OpenTelemetry-ready tracing and metrics via in-box
  `System.Diagnostics` APIs (`AddSource`/`AddMeter` on `"Runax.Messaging"`), and
  per-transport health checks.
- **`Runax.Messaging.Outbox`** — transactional outbox that persists publishes in
  your database transaction and dispatches them from a background service.
- **`Runax.Messaging.TestKit`** — a broker-free `MessagingTestHarness` to publish
  messages and assert what consumers handled, retried, or dead-lettered.

[1.0.0]: https://github.com/runax-software/runax-messaging/releases/tag/v1.0.0
