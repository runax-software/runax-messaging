# Runax.Messaging — Roadmap / TODO

Enhancement backlog, ordered by how much each adds to a production-grade OSS
messaging library. File/line references point at where the gap lives today.

## Tier 1 — Reliability  ✅ done

- [x] **Retry + dead-letter / poison-message handling.** Dispatch pipeline now
      retries with exponential backoff (`RetryOptions`) and republishes exhausted
      or poison messages to `{topic}.dead-letter` with `x-runax-dlq-*` headers
      (`MessageConsumerHostedService.cs`).
- [x] **Ack/nack control for consumers.** Transports act on a returned
      `MessageDisposition` (`Acknowledge`/`Requeue`); consumers signal permanent
      failure by throwing `PoisonMessageException` to skip retries.
- [x] **RabbitMQ delivery guarantees.** Publisher confirms
      (`WaitForConfirmsOrDie`), `BasicQos` prefetch, and automatic + topology
      recovery are enabled; ack/nack is driven by the disposition
      (`RabbitMqTransport.cs`).

## Tier 2 — Observability  ✅ done

- [x] **OpenTelemetry tracing.** `MessagingDiagnostics` exposes an `ActivitySource`
      ("Runax.Messaging"); publish emits a producer span and injects W3C context into the
      envelope headers via `DistributedContextPropagator`, and consume extracts it into a
      consumer span. Spans carry `messaging.system`/`messaging.destination.name`/`messaging.operation`.
- [x] **Metrics** via a `Meter` ("Runax.Messaging"): `runax.messaging.published`/`consumed`/`failed`
      counters and a `runax.messaging.processing.duration` histogram, tagged by system and destination.
- [x] **Health checks.** `AddRabbitMqTransport()` / `AddSqsTransport()` extend
      `IHealthChecksBuilder` with broker-reachability checks that probe the live transport connection.

## Tier 3 — Configuration & DX

- [ ] **Real options plumbing.** Options are registered as bare singletons from a
      `configure` action (`RabbitMqConfiguratorExtensions.cs`); no `IOptions<T>`,
      validation, or `IConfiguration` binding. `Microsoft.Extensions.Options.ConfigurationExtensions`
      is already staged in `Directory.Packages.props` but unused. Add
      `ValidateDataAnnotations`/`IValidateOptions` + configuration-binding overloads.
- [ ] **Pluggable serialization.** `MessageContext.Deserialize<T>()` bypasses the
      configured `IMessageSerializer` and uses default `JsonSerializer` options
      (`MessageContext.cs`). Surface configurable `JsonSerializerOptions` and add a
      source-generated / AOT-trim-friendly path.
- [ ] **RabbitMQ security surface.** Options carry a plaintext password with no AMQP
      URI or TLS configuration (`RabbitMqOptions.cs`).

## Tier 4 — Throughput & scale

- [ ] **Concurrent SQS consumption.** `SqsTransport.SubscribeAsync` polls queues one
      at a time and processes messages sequentially. Add parallel per-queue pumps and
      a configurable concurrency limit.
- [ ] **Batch publish** (SQS `SendMessageBatch`, RabbitMQ batching).
- [ ] **Transactional outbox** as a separate `Runax.Messaging.Outbox` package
      (DB write + publish atomicity).

## Tier 5 — Surface area / ecosystem

- [ ] **Implement staged transports.** Kafka, Google Pub/Sub, and Azure Service Bus
      versions are pinned in `Directory.Packages.props` but have no project yet.
- [ ] **RabbitMQ.Client 6.x → 7.x.** Migrate off the deprecated synchronous `IModel`
      API to v7's async `IChannel`; unlocks true async publish.
- [ ] **`Runax.Messaging.TestKit`.** The in-memory transport exists but is `internal`;
      expose a public test harness for asserting consumer behavior.

---

Note: Tiers 1 and 2 have landed and expanded the public/provider surface —
`IMessagingTransport` now exposes `SystemName` and `SubscribeAsync` returns a `MessageDisposition`
that includes `DeadLetter`; `MessagingDiagnostics` (activity source + meter names) and the
per-transport health-check builder extensions are also public. Review this surface before tagging
`1.0` and freezing the API.
