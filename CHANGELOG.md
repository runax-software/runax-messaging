# Changelog

All notable changes to Runax.Messaging are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
All packages in the repository are versioned together.

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
