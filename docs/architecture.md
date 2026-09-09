# Architecture & message flow

Runax.Messaging separates the messaging *contract* from its *implementation* and
from each *transport*, so applications depend on stable abstractions while
brokers stay swappable. Configuration is organized around **buses**: a bus is a
named messaging context wrapping exactly one transport, plus the consumers and
policies around it (see [migrating to 2.0](migrating-to-2.0.md)).

## Package layering

```
Runax.Messaging.Abstractions      (contracts: IBus, IBusProvider, IMessagingTransport, TransportConfig, MessageContext)
        ▲                 ▲
        │                 │
Runax.Messaging      Runax.Messaging.Transports.Aws.Sqs / .RabbitMq / <your transport>
(impl + in-memory)   (implement IMessagingTransport, ship a TransportConfig-derived config type)
```

- **Abstractions** depends on nothing but `Microsoft.Extensions.DependencyInjection.Abstractions`.
- **Core** references Abstractions and provides the `AddBus` / `BusBuilder` wiring,
  serialization, the bus publish pipeline, the hosted consumer dispatcher, and the
  in-memory transport.
- **Transports** reference Abstractions only — never each other, and never the
  core implementation.
- **Higher-level packages** may reference Core — e.g. `Runax.Messaging.Outbox`
  reuses the serializer and swaps a bus's publish sink.

## Key types

| Type | Package | Responsibility |
| --- | --- | --- |
| `IBus` | Abstractions | The single application handle to messaging: `Name`, `Mode`, and the publish API (`PublishAsync`, `PublishBatchAsync`). Unkeyed injection resolves the default bus; keyed injection (`[FromKeyedServices("name")]`) a named one. |
| `IBusProvider` | Abstractions | Dynamic bus lookup (`GetBus("<name>")`) and enumeration (`Buses`) for diagnostics and admin surfaces. |
| `BusMode` / `BusNames` | Abstractions | A bus's declared mode (`PublishAndConsume` / `PublishOnly` / `ConsumeOnly`) and the well-known default bus name. |
| `TransportConfig` / `TransportContext` | Abstractions | Transport SPI: a transport package derives one config type carrying its broker settings (DataAnnotations-validated) and the `CreateTransport` factory; the context hands it the bus name, mode, and service provider. |
| `IMessagingTransport` | Abstractions | Provider SPI: broker-specific publish (single + `PublishBatchAsync`) / subscribe over serialized envelopes, plus a `SystemName` telemetry tag. |
| `MessageContext` | Abstractions | A received message (topic, body, headers) with `Deserialize<T>()`. |
| `MessageDisposition` | Abstractions | The verdict a transport applies after dispatch: `Acknowledge`, `Requeue`, or `DeadLetter`. |
| `PoisonMessageException` | Abstractions | Thrown by a consumer to skip retries and dead-letter the message immediately. |
| `MessageContractAttribute` | Abstractions | Opt-in `[MessageContract(version)]` declaring a message type's contract version (and optional name). |
| `IUnroutableMessageHandler` | Abstractions | Decides the fate of a message no consumer accepts; built-ins via `bus.OnUnroutableMessage(...)`. |
| `MessagingConfigurator` | Abstractions | The `AddRunaxMessaging` surface; buses attach via the `AddBus` extensions in the core package. |
| `BusBuilder` | Core | Configures one bus inside an `AddBus` block: its one transport, consumers, mode, and retry/serialization/unroutable policies. Validated when the block completes. |
| `Bus` | Core (internal) | The `IBus` implementation: a small publish pipeline — mode guard → serializer (per-topic/per-bus) → sink (the transport, or the outbox store when configured) — with publish telemetry. |
| `IMessageContractCatalog` | Core | Introspects which topics/versions the app handles (`Handled`, `Accepts(topic, version)`). |
| `RetryOptions` / `DeadLetterStrategy` | Core | Retry backoff and dead-letter policy applied by the dispatcher (`bus.WithRetry`). |
| `MessagingDiagnostics` | Core | The `ActivitySource` and `Meter` names for tracing and metrics. |
| `ISerializer` | Core | Pluggable **body** serializer (`bus.UseSerializer<T>()`) — controls how bodies are encoded, not the envelope. The default is System.Text.Json. |
| `IMessageSerializer` | Core (framework-owned) | Frames the reserved `__runax` envelope around the body; not a customization point. |
| `MessageConsumer<TMessage>` | Core | Base class for a typed consumer of a single topic. |
| `MessageConsumerHostedService` | Core (internal) | Background service — one instance **per consuming bus** — that subscribes that bus's consumers and dispatches messages with retry, dead-lettering, and telemetry. |

## The envelope

Messages travel wrapped in an envelope so metadata rides alongside the payload:

```json
{
  "Id": 1,
  "Name": "widget",
  "__runax": {
    "contract_name": "orders.placed",
    "contract_version": 2,
    "headers": { "correlation-id": "abc" }
  }
}
```

The payload sits at the top level and framework metadata rides under the reserved `__runax` key. This makes
the envelope **self-identifying** (presence of `__runax` = a Runax message) and interop cheap in both
directions: a payload with **no** `__runax` — an S3 event, another producer's JSON — is read as a plain body,
and foreign consumers see a normal object. `contract_name` / `contract_version` appear only when the message
type carries `[MessageContract]` (see [Contract versioning](#contract-versioning)). The transport only ever sees the
serialized string — it never needs to know your message types. The body serializer is pluggable (the `__runax`
envelope is always framework-owned); see [Serialization & custom serializers](serialization.md). The envelope
is unchanged from 1.x, so 1.x and 2.0 services interoperate on the same broker.

## Publish flow

```
bus.PublishAsync(topic, message[, headers])
        └─ Bus publish pipeline: mode guard (ConsumeOnly → throw); start producer span,
           inject W3C trace context into headers
                └─ serialize (message + headers) → envelope JSON (per-topic / per-bus serializer)
                        └─ sink: IMessagingTransport.PublishAsync(topic, envelopeJson)
                           (or the IOutboxStore when the bus has an outbox — see below)
                                └─ broker (queue / exchange / channel)
```

## Consume flow

```
Host starts → one MessageConsumerHostedService per consuming bus
        ├─ resolve the bus's registered consumers, group by Topic
        └─ IMessagingTransport.SubscribeAsync(topics, onMessage) ──▶ returns MessageDisposition
                └─ per message:
                        ├─ deserialize envelope → MessageContext; extract trace context → consumer span
                        ├─ select the topic's consumers matching the contract version (see below)
                        │       └─ none match → IUnroutableMessageHandler decides (dead-letter by default)
                        ├─ MessageConsumer<T>: deserialize Body → T → HandleAsync(T)
                        │       └─ on failure: retry with backoff, then dead-letter (see below)
                        └─ return Acknowledge / Requeue / DeadLetter to the transport
```

Consumers are ordinary DI singletons. Dispatch runs inside a hosted
`BackgroundService` per bus, so buses subscribe, run, and shut down independently — a
transport failure on one bus does not touch the others — and a `PublishOnly` bus starts
no hosted service at all. Consuming requires a .NET Generic Host; publishing has no
such requirement.

## Contract versioning

Versioning is opt-in and envelope-level, so it is transport-agnostic and composes with everything above.

- **Identity.** `[MessageContract(version)]` on a message type stamps `Contract` (the optional name, else
  the topic is the effective identity) and `ContractVersion` into the envelope. Types without the attribute
  are unversioned — the envelope fields stay `null` and behavior is unchanged.
- **Routing.** The dispatcher matches a message to the topic's consumers by version: a versioned consumer
  (`MessageConsumer<T>` whose `T` is a contract) accepts only its own version, while an unversioned consumer
  accepts every message on the topic. This lets several versions — one consumer each — coexist on one topic,
  each receiving its exact type at full fidelity.
- **Unroutable messages.** When no consumer accepts a version, an `IUnroutableMessageHandler` decides:
  `bus.OnUnroutableMessage(UnroutableStrategy.DeadLetter | Requeue | Discard)` (default dead-letter) or a
  custom handler. `DeadLetter` runs through the same `DeadLetterStrategy` below, so nothing is silently dropped.
- **Coverage.** `IMessageContractCatalog` reports the handled `(topic, version)` pairs so an app can verify
  it consumes a version before a producer begins emitting it.

## Reliability: retries & dead-lettering

The dispatcher (`MessageConsumerHostedService`) applies a uniform, transport-agnostic
policy around every `HandleAsync`, configured with `bus.WithRetry(...)` (`RetryOptions`).
`WithRetry` and `OnUnroutableMessage` are **bus-scoped**, with `WithRetryForTopic` as the
per-topic override and the built-in defaults as the fallback; see
[Configuration & per-bus settings](configuration.md).

- **Retry.** A failed `HandleAsync` is retried up to `MaxAttempts` with exponential
  backoff (`InitialDelay` × `BackoffFactor`, capped at `MaxDelay`).
- **Poison messages.** A consumer that throws `PoisonMessageException` skips retries
  and is dead-lettered immediately.
- **Dead-lettering.** When retries are exhausted (or a message is poison or its
  envelope is malformed), `DeadLetterStrategy` decides what happens:
  - `FrameworkManaged` (default) republishes the message to `{topic}.dead-letter`
    with `x-runax-dlq-*` headers, then acknowledges the original. Works on every transport.
    This framework-internal publish is part of consuming, so it remains allowed on a
    `ConsumeOnly` bus.
  - `BrokerNative` returns `MessageDisposition.DeadLetter` so the broker's own
    dead-letter facility handles it. Pair it with `MaxAttempts = 1` to rely purely
    on the broker for retries.

The dispatcher returns a `MessageDisposition` that each transport maps to a broker action:

| Disposition | RabbitMQ | SQS | In-memory |
| --- | --- | --- | --- |
| `Acknowledge` | `basic.ack` | `DeleteMessage` | drop |
| `Requeue` | `basic.nack` (requeue) | leave for the visibility timeout | re-enqueue |
| `DeadLetter` | `basic.nack` (no requeue) → dead-letter exchange | leave for the redrive policy | drop |

## Observability

Instrumentation uses the in-box `System.Diagnostics` primitives — no OpenTelemetry
SDK dependency. Consumers subscribe by name (`MessagingDiagnostics.ActivitySourceName`
/ `MeterName`, both `"Runax.Messaging"`):

- **Tracing.** Publish starts a `Producer` span and injects W3C trace context into the
  envelope headers; consume extracts it and starts a `Consumer` span. Spans carry the
  `messaging.system` / `messaging.destination.name` / `messaging.operation` tags, plus
  `messaging.runax.bus` (the bus name) — the tag that disambiguates two buses on the
  same broker type on one dashboard.
- **Metrics.** `runax.messaging.published` / `consumed` / `failed` counters and a
  `runax.messaging.processing.duration` histogram, tagged by system, destination, and bus.
- **Health checks.** Each bus auto-registers a broker-reachability check named
  `runax:{bus}` (opt out with `RegisterHealthCheck = false` on the transport config;
  the in-memory transport has no check).

Wire them into OpenTelemetry with `AddSource("Runax.Messaging")` and
`AddMeter("Runax.Messaging")`.

## Throughput

- **Batch publish.** `IBus.PublishBatchAsync(topic, messages)` serializes each
  message under one producer span and calls `IMessagingTransport.PublishBatchAsync`. The SPI
  method has a default (sequential) implementation; SQS overrides it with `SendMessageBatch`
  (chunks of 10) and RabbitMQ publishes the whole batch on one channel with a single confirm.
- **Concurrent consumption.** The SQS transport runs one pump per queue and dispatches messages
  continuously up to `MaxConcurrentMessages` (a shared `SemaphoreSlim`), so successive polls
  overlap. The dispatch pipeline itself is unchanged — each message still returns a `MessageDisposition`.

## Transactional outbox

The optional `Runax.Messaging.Outbox` package makes "save data + publish" atomic. An outbox
belongs to one bus: `bus.AddOutbox()` swaps that bus's publish-pipeline **sink**, so
`bus.PublishAsync` serializes and writes to the registered `IOutboxStore` instead of the
transport; a background `OutboxDispatcher` (one per outbox bus) later drains pending rows to
the bus's transport and marks them dispatched. The store is registered with
`bus.AddOutboxStore(...)` via an `OutboxStoreConfig`-derived config type — the same uniform
pattern as `AddTransport` — and the outbox/store pairing is validated when the `AddBus` block
completes. A durable store's `AddAsync` enlists in the caller's database transaction, so the
message row commits together with the business data (at-least-once delivery — keep consumers
idempotent). Buses without an outbox publish straight to their transport. See the
[package README](../src/Runax.Messaging.Outbox/README.md).

## Design rules

- Transports depend on **Abstractions only**. Keep broker SDKs out of the core.
- **One bus = one transport**, enforced at configuration time. Several brokers — or several
  clusters of the same broker — are several buses, each with its own connection, options
  validation, health check, and telemetry identity. `SystemName` is a telemetry tag, not an
  identity, and no longer needs to be unique.
- A consumer belongs to the bus it is registered on (`bus.AddConsumer<T>()`); the hosted
  dispatcher subscribes and dispatches each bus independently. Register the same consumer type
  on several buses to consume from each of them.
- Scoped settings (`WithRetry`, `OnUnroutableMessage`, `ConfigureSerialization`,
  `UseSerializer`) resolve **bus → topic**, most specific first: the per-topic value
  (`*ForTopic`) if set, else the bus value, else the built-in default. There is no global
  scope and no cross-bus leakage. See [Configuration & per-bus settings](configuration.md).
- Applications depend on `IBus` (from Abstractions), not on any concrete transport.
