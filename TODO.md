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
- [ ] `**Runax.Messaging.TestKit`.\*\* The in-memory transport exists but is `internal`;
    ```
    expose a public test harness for asserting consumer behavior.
    ```
