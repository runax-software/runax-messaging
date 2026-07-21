# Runax.Messaging — Roadmap / TODO

Enhancement backlog, ordered by how much each adds to a production-grade OSS
messaging library. File/line references point at where the gap lives today.

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

Note: Tiers 1–3 have landed and expanded the public/provider surface —
`IMessagingTransport` now exposes `SystemName` and `SubscribeAsync` returns a `MessageDisposition`
that includes `DeadLetter`; `MessagingDiagnostics` (activity source + meter names), the
per-transport health-check builder extensions, the `IConfiguration`-binding transport overloads,
and `ConfigureSerialization` are also public. Review this surface before tagging `1.0` and
freezing the API.
