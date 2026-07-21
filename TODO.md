# Runax.Messaging — Roadmap / TODO

Enhancement backlog, ordered by how much each adds to a production-grade OSS
messaging library. File/line references point at where the gap lives today.

## Tier 5 — Surface area / ecosystem

Full publish + subscribe transports (implement `IMessagingTransport`), under the
`Runax.Messaging.Transports.*` namespace (cloud providers get a vendor segment; standalone brokers
are flat). Sibling brokers to those already shipped by `runax-hookpipe`:

- [ ] **Kafka transport** (`Runax.Messaging.Transports.Kafka`). `Confluent.Kafka` is pinned in
      `Directory.Packages.props` but has no project yet.
- [ ] **Azure Service Bus transport** (`Runax.Messaging.Transports.Azure.ServiceBus`).
      `Azure.Messaging.ServiceBus` is pinned in `Directory.Packages.props` but has no project yet.
- [ ] **Azure Event Hubs transport** (`Runax.Messaging.Transports.Azure.EventHubs`). Needs
      `Azure.Messaging.EventHubs` pinned; consume via a consumer group + checkpoint store.
- [ ] **AWS SNS transport** (`Runax.Messaging.Transports.Aws.Sns`). Needs
      `AWSSDK.SimpleNotificationService` pinned. SNS fans out to subscribers, so pair it with SQS
      for the consume side (SNS→SQS).

Publish-only / relay sinks that `runax-hookpipe` exposes but do **not** fit the bidirectional
`IMessagingTransport` SPI (no poll/subscribe model). Revisit only if a publish-only transport
abstraction is introduced: AWS EventBridge, HTTP relay, stdout.

- [ ] **`Runax.Messaging.TestKit`.** The in-memory transport exists but is `internal`;
      expose a public test harness for asserting consumer behavior.

---

Note: Tiers 1–4 have landed and expanded the public/provider surface —
`IMessagingTransport` now exposes `SystemName`, `PublishBatchAsync`, and a `SubscribeAsync` that
returns a `MessageDisposition` including `DeadLetter`; `IMessagePublisher.PublishBatchAsync<T>`,
`MessagingDiagnostics`, the per-transport health-check builder extensions, the `IConfiguration`-binding
transport overloads, `ConfigureSerialization`, and the whole `Runax.Messaging.Outbox` package are also
public. Review this surface before tagging `1.0` and freezing the API.

Naming: transport packages now live under `Runax.Messaging.Transports.*` — the RabbitMQ and SQS
packages were renamed to `Runax.Messaging.Transports.RabbitMq` and `Runax.Messaging.Transports.Aws.Sqs`.
