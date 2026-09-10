# Runax.Messaging — Roadmap

Post-1.0 enhancement backlog, ordered by how much each adds to a production-grade
OSS messaging library. Nothing here blocks a release; 1.0.0 shipped the full
transport set, outbox, TestKit, contract versioning, reliability, and
observability (see [CHANGELOG.md](CHANGELOG.md)).

## Core enhancements

- [ ] **Inbox / idempotent consumer** — consume-side deduplication, the natural
  partner to the transactional outbox and the biggest remaining gap for
  exactly-once-ish delivery.
- [ ] **Scheduling (delayed / scheduled messages)** — deliver a message at or
  after a chosen time: broker-native where supported (SQS delay, Service Bus
  scheduled enqueue), scheduler-emulated elsewhere. Also underpins saga timeouts
  and delayed retries.
- [ ] **Request/response** — correlated RPC over the broker: `await` a reply to a
  published request, matched back via a correlation ID and a temporary reply
  queue, giving synchronous-feeling calls on an async transport.
- [ ] **In-process mediator** — the same publish/consume (and request/response)
  programming model with no broker or transport; handlers run in-memory within
  the process, for local decoupling without a network hop.
- [ ] **Sagas / state machines** — persisted, long-running workflow
  orchestration: a saga reacts to a stream of events over time and coordinates a
  multi-step process, with timeouts and compensation on failure. Depends on a
  saga state store and the scheduling work above (for timeouts).
- [ ] **AOT / trim support** — mark the shippable projects `IsTrimmable` /
  `IsAotCompatible` and validate; the serializer is already source-gen-capable.
- [ ] **Benchmarks (BenchmarkDotNet)** — guard throughput and allocations against
  regressions as the library grows.



## Transports

- [ ] **ActiveMQ transport** (`Runax.Messaging.Transports.ActiveMq`) — publish/
  consume over ActiveMQ (Artemis / classic) via the `IMessagingTransport` SPI.
- [ ] **BullMQ transport** (`Runax.Messaging.Transports.BullMq`) — interop with
  BullMQ job queues over Redis: enqueue jobs that Node.js BullMQ workers pick up,
  and consume jobs enqueued from Node. Unlike the other transports this targets a
  library's Redis data-structure convention rather than a broker protocol, so it
  must stay wire-compatible with BullMQ's key layout and Lua scripts (pin and
  test against specific BullMQ versions).
- [ ] **MQTT transport** (`Runax.Messaging.Transports.Mqtt`) — publish/consume
  over standard MQTT (3.1.1 / 5) brokers such as Mosquitto, EMQX, or HiveMQ via
  MQTTnet. Competing-consumer semantics need MQTT 5 shared subscriptions
  (`$share/{group}/{topic}`); on 3.1.1 brokers each consumer group member sees
  every message, so document that limitation up front.



## Nice-to-have

- [ ] **Routing slips** *(tentative)* — choreographed multi-step transactions:
  the message carries an itinerary of activities, each recording a compensation
  to roll back on failure. The choreographed counterpart to sagas; revisit once
  sagas land.
- [ ] **Multi-bus fan-out publish** *(tentative)* — a one-call helper
  (`BroadcastAsync`) to send on several buses at once, layered over the  
  `IBusProvider` / keyed `IBus` resolution that already ships.  
  Design is deferred pending two decisions: (1) the same-contract case (one  
  message mirrored to N buses) and the different-contract case (each  
  bus gets its own topic + payload, e.g. `user.order` differing on the Kafka bus  
  vs the SQS bus) want different shapes — likely a per-bus builder of independent  
  `(bus, topic, message)` entries rather than a single-message overload;  
  (2) a partial-failure policy (fail-fast vs. best-effort with an aggregate),  
  since the sends are independent and at-least-once. Until then, inject each  
  bus (`[FromKeyedServices("<name>")] IBus`, or `IBusProvider.GetBus(...)`) and  
  publish on each explicitly.
- [ ] **JSON Schema contracts** (`Runax.Messaging.Contracts.JsonSchema`) — a
  language-neutral schema per contract, keyed by the `(contract_name,
  contract_version)` pair that already travels in the `__runax` envelope (no
  wire changes). Two independently adoptable pieces: (1) a **CI compatibility
  check** — export schemas from `[MessageContract]` types (or accept
  hand-authored schemas as the source of truth) and diff them across versions,
  turning the additive-vs-breaking evolution rules in
  [docs/contracts.md](docs/contracts.md) into a machine-checked gate alongside
  the `IMessageContractCatalog` rollout check; (2) **opt-in payload
  validation** on publish and/or consume via the pluggable `ISerializer` seam
  — off by default, since per-message validation is not free. Also formalizes
  the duplicated-types / cross-language story (today wire compatibility of
  copies is by convention only). Schema-registry integrations (Confluent SR,
  AWS Glue) stay tentative and out of the core feature: they are
  broker-ecosystem-specific, while this must remain transport-agnostic.
- [ ] **CloudEvents serializer** — optional serializer emitting/consuming the
  CloudEvents envelope for interop.

