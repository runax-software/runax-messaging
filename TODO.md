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

Follow-ups deferred from the Tier 1 implementation:

- [ ] **Broker-native dead-lettering.** Current DLQ is framework-managed republish
      to `{topic}.dead-letter` (uniform, testable). Optionally map `Requeue`/reject to
      RabbitMQ DLX and SQS redrive policies for operators who prefer native DLQs.
- [ ] **RabbitMQ publish throughput.** Publishing uses a single `Lock`-guarded channel
      (`IModel` is not thread-safe). Consider a channel pool if publish becomes a bottleneck.
- [ ] **SQS retry vs. visibility timeout.** In-process retry backoff runs against the
      message's visibility timeout; keep delays small or lean on native redrive. Consider
      extending visibility (`ChangeMessageVisibility`) during retries.

## Tier 2 — Observability

- [ ] **OpenTelemetry tracing.** Add an `ActivitySource`; propagate W3C `traceparent`
      through the envelope headers (`MessageEnvelope.Headers`) across publish→consume.
      Follow OTel messaging semantic conventions for producer/consumer spans.
- [ ] **Metrics** via `System.Diagnostics.Metrics.Meter`: published/consumed/failed
      counters + processing-duration histogram.
- [ ] **Health checks.** `Microsoft.Extensions.Diagnostics.HealthChecks` integration
      per transport (broker reachability).

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

Note: Tier 1 has landed and already changed the provider SPI
(`IMessagingTransport.SubscribeAsync` now returns `MessageDisposition`). Tier 2 also
touches the public surface, so land it before tagging `1.0` and freezing the API.
