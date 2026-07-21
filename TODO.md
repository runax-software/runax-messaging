# Runax.Messaging — Roadmap / TODO

Enhancement backlog, ordered by how much each adds to a production-grade OSS
messaging library. File/line references point at where the gap lives today.

## Tier 5 — Surface area / ecosystem

- [ ] **Implement staged transports.** Kafka, Google Pub/Sub, and Azure Service Bus
      versions are pinned in `Directory.Packages.props` but have no project yet.
- [ ] **`Runax.Messaging.TestKit`.** The in-memory transport exists but is `internal`;
      expose a public test harness for asserting consumer behavior.

---

Note: Tiers 1–4 have landed and expanded the public/provider surface —
`IMessagingTransport` now exposes `SystemName`, `PublishBatchAsync`, and a `SubscribeAsync` that
returns a `MessageDisposition` including `DeadLetter`; `IMessagePublisher.PublishBatchAsync<T>`,
`MessagingDiagnostics`, the per-transport health-check builder extensions, the `IConfiguration`-binding
transport overloads, `ConfigureSerialization`, and the whole `Runax.Messaging.Outbox` package are also
public. Review this surface before tagging `1.0` and freezing the API.
