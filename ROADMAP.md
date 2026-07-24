# Runax.Messaging — Roadmap

Post-1.0 enhancement backlog, ordered by how much each adds to a production-grade
OSS messaging library. Nothing here blocks a release; 1.0.0 shipped the full
transport set, outbox, TestKit, contract versioning, reliability, and
observability (see [CHANGELOG.md](CHANGELOG.md)).

## 1.x candidates

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

## Nice-to-have

- [ ] **Routing slips** *(tentative)* — choreographed multi-step transactions:
  the message carries an itinerary of activities, each recording a compensation
  to roll back on failure. The choreographed counterpart to sagas; revisit once
  sagas land.
- [ ] **Per-topic serializer** — serializer selection per topic (global and
  per-broker selection already ship).
- [ ] **CloudEvents serializer** — optional serializer emitting/consuming the
  CloudEvents envelope for interop.
