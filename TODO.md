# Runax.Messaging — Roadmap / TODO

Enhancement backlog, ordered by how much each adds to a production-grade OSS
messaging library. File/line references point at where the gap lives today.

## Tier 5 — Surface area / ecosystem

Full publish + subscribe transports (implement `IMessagingTransport`), under the
`Runax.Messaging.Transports.*` namespace (cloud providers get a vendor segment; standalone brokers
are flat). Sibling brokers to those already shipped by `runax-hookpipe`:

- [ ] **Kafka transport** (`Runax.Messaging.Transports.Kafka`). `Confluent.Kafka` is pinned in
  ```
  `Directory.Packages.props` but has no project yet.
  ```
- [ ] **Azure Event Hubs transport** (`Runax.Messaging.Transports.Azure.EventHubs`). Needs
  ```
  `Azure.Messaging.EventHubs` pinned; consume via a consumer group + checkpoint store.
  ```
- [ ] `**Runax.Messaging.TestKit`.** The in-memory transport exists but is `internal`;
  ```
  expose a public test harness for asserting consumer behavior.
  ```

## Tier 6 — Contracts & advanced routing

- [ ] **Message contracts + versioning.** Support declaring versioned message contracts and evolving
      them safely: carry a contract/version tag in the envelope, add resolver/upcaster hooks that
      migrate an older payload to the current type, and make (de)serialization backward/forward
      compatible so a consumer can accept more than one contract version.
- [ ] **Multi-transport consumers.** Allow registering a single consumer against several brokers at
      once — which may be *different* transports (e.g. RabbitMQ + SQS). Requires lifting the current
      "exactly one `IMessagingTransport`" assumption to a keyed/named set of transports and letting a
      consumer (or the hosted dispatcher) subscribe across more than one.

