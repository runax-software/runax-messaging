# Runax.Messaging — Roadmap / TODO

Enhancement backlog, ordered by how much each adds to a production-grade OSS
messaging library. File/line references point at where the gap lives today.

## Tier 4 — Throughput & scale  ✅ done

- [x] **Concurrent SQS consumption.** `SqsTransport.SubscribeAsync` runs one pump per queue in
      parallel and dispatches messages continuously, bounded by `MaxConcurrentMessages` (a shared
      `SemaphoreSlim`), so successive polls overlap instead of processing one batch at a time.
- [x] **Batch publish.** `IMessagingTransport.PublishBatchAsync` (default sequential loop) is
      overridden by SQS (`SendMessageBatch` in chunks of 10) and RabbitMQ (publish all, then a single
      confirm). Surfaced as `IMessagePublisher.PublishBatchAsync<T>`.
- [x] **Transactional outbox.** New `Runax.Messaging.Outbox` package: `AddOutbox()` routes
      `IMessagePublisher` through an `IOutboxStore` and a background `OutboxDispatcher` drains it to the
      transport. Ships an `InMemoryOutboxStore`; durable stores implement `IOutboxStore.AddAsync` to
      enlist in the caller's DB transaction.

## Tier 5 — Surface area / ecosystem

- [ ] **Implement staged transports.** Kafka, Google Pub/Sub, and Azure Service Bus
      versions are pinned in `Directory.Packages.props` but have no project yet.
- [ ] **RabbitMQ.Client 6.x → 7.x.** Migrate off the deprecated synchronous `IModel`
      API to v7's async `IChannel`; unlocks true async publish.
- [ ] **`Runax.Messaging.TestKit`.** The in-memory transport exists but is `internal`;
      expose a public test harness for asserting consumer behavior.

---

Note: Tiers 1–4 have landed and expanded the public/provider surface —
`IMessagingTransport` now exposes `SystemName`, `PublishBatchAsync`, and a `SubscribeAsync` that
returns a `MessageDisposition` including `DeadLetter`; `IMessagePublisher.PublishBatchAsync<T>`,
`MessagingDiagnostics`, the per-transport health-check builder extensions, the `IConfiguration`-binding
transport overloads, `ConfigureSerialization`, and the whole `Runax.Messaging.Outbox` package are also
public. Review this surface before tagging `1.0` and freezing the API.
