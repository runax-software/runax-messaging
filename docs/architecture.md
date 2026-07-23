# Architecture & message flow

Runax.Messaging separates the messaging *contract* from its *implementation* and
from each *transport*, so applications depend on stable abstractions while
brokers stay swappable.

## Package layering

```
Runax.Messaging.Abstractions      (contracts: IMessagePublisher, IMessagingTransport, MessageContext, MessagingConfigurator)
        ▲                 ▲
        │                 │
Runax.Messaging      Runax.Messaging.Transports.Aws.Sqs / .RabbitMq / <your transport>
(impl + in-memory)   (implement IMessagingTransport, add a configurator extension)
```

- **Abstractions** depends on nothing but `Microsoft.Extensions.DependencyInjection.Abstractions`.
- **Core** references Abstractions and provides serialization, the publisher
  adapter, the hosted consumer dispatcher, and the in-memory transport.
- **Transports** reference Abstractions only — never each other, and never the
  core implementation.
- **Higher-level packages** may reference Core — e.g. `Runax.Messaging.Outbox`
  reuses the serializer and decorates `IMessagePublisher`.

## Key types

| Type | Package | Responsibility |
| --- | --- | --- |
| `IMessagePublisher` | Abstractions | The publish API applications call (`PublishAsync`, `PublishBatchAsync`). |
| `IMessagingTransport` | Abstractions | Provider SPI: broker-specific publish (single + `PublishBatchAsync`) / subscribe over serialized envelopes, plus a `SystemName` telemetry tag. |
| `MessageContext` | Abstractions | A received message (topic, body, headers) with `Deserialize<T>()`. |
| `MessageDisposition` | Abstractions | The verdict a transport applies after dispatch: `Acknowledge`, `Requeue`, or `DeadLetter`. |
| `PoisonMessageException` | Abstractions | Thrown by a consumer to skip retries and dead-letter the message immediately. |
| `MessageContractAttribute` | Abstractions | Opt-in `[MessageContract(version)]` declaring a message type's contract version (and optional name). |
| `IUnroutableMessageHandler` | Abstractions | Decides the fate of a message no consumer accepts; built-ins via `OnUnroutableMessage(...)`. |
| `IMessageContractCatalog` | Core | Introspects which topics/versions the app handles (`Handled`, `Accepts(topic, version)`). |
| `MessagingConfigurator` | Abstractions | Fluent builder; transports and consumers attach via extensions. |
| `RetryOptions` / `DeadLetterStrategy` | Core | Retry backoff and dead-letter policy applied by the dispatcher (`WithRetry`). |
| `MessagingDiagnostics` | Core | The `ActivitySource` and `Meter` names for tracing and metrics. |
| `MessagePublisherAdapter` | Core (internal) | Bridges `IMessagePublisher` → `IMessagingTransport`, serializing to an envelope and emitting publish telemetry. |
| `IMessageSerializer` | Core | Pluggable wire codec (`UseSerializer<T>()`); the default puts the payload at the top level with metadata under `__runax`. |
| `MessageConsumer<TMessage>` | Core | Base class for a typed consumer of a single topic. |
| `MessageConsumerHostedService` | Core (internal) | Background service that subscribes consumers and dispatches messages with retry, dead-lettering, and telemetry. |

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
serialized string — it never needs to know your message types. The wire format is pluggable; see
[Serialization & custom serializers](serialization.md).

## Publish flow

```
publisher.PublishAsync(topic, message[, headers])
        └─ MessagePublisherAdapter: start producer span, inject W3C trace context into headers
                └─ serialize (message + headers) → envelope JSON
                        └─ IMessagingTransport.PublishAsync(topic, envelopeJson)
                                └─ broker (queue / exchange / channel)
```

## Consume flow

```
Host starts → MessageConsumerHostedService.ExecuteAsync
        ├─ resolve each registered consumer, group by Topic
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
`BackgroundService`, so consuming requires a .NET Generic Host. Publishing has no
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
  `OnUnroutableMessage(UnroutableStrategy.DeadLetter | Requeue | Discard)` (default dead-letter) or a custom
  handler. `DeadLetter` runs through the same `DeadLetterStrategy` below, so nothing is silently dropped.
- **Coverage.** `IMessageContractCatalog` reports the handled `(topic, version)` pairs so an app can verify
  it consumes a version before a producer begins emitting it.

## Reliability: retries & dead-lettering

The dispatcher (`MessageConsumerHostedService`) applies a uniform, transport-agnostic
policy around every `HandleAsync`, configured with `WithRetry(...)` (`RetryOptions`):

- **Retry.** A failed `HandleAsync` is retried up to `MaxAttempts` with exponential
  backoff (`InitialDelay` × `BackoffFactor`, capped at `MaxDelay`).
- **Poison messages.** A consumer that throws `PoisonMessageException` skips retries
  and is dead-lettered immediately.
- **Dead-lettering.** When retries are exhausted (or a message is poison or its
  envelope is malformed), `DeadLetterStrategy` decides what happens:
  - `FrameworkManaged` (default) republishes the message to `{topic}.dead-letter`
    with `x-runax-dlq-*` headers, then acknowledges the original. Works on every transport.
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
  `messaging.system` / `messaging.destination.name` / `messaging.operation` tags.
- **Metrics.** `runax.messaging.published` / `consumed` / `failed` counters and a
  `runax.messaging.processing.duration` histogram, tagged by system and destination.
- **Health checks.** Each transport package ships an `IHealthChecksBuilder` extension
  (`AddRabbitMqTransport()`, `AddSqsTransport()`) that probes broker reachability.

Wire them into OpenTelemetry with `AddSource("Runax.Messaging")` and
`AddMeter("Runax.Messaging")`.

## Throughput

- **Batch publish.** `IMessagePublisher.PublishBatchAsync(topic, messages)` serializes each
  message under one producer span and calls `IMessagingTransport.PublishBatchAsync`. The SPI
  method has a default (sequential) implementation; SQS overrides it with `SendMessageBatch`
  (chunks of 10) and RabbitMQ publishes the whole batch on one channel with a single confirm.
- **Concurrent consumption.** The SQS transport runs one pump per queue and dispatches messages
  continuously up to `MaxConcurrentMessages` (a shared `SemaphoreSlim`), so successive polls
  overlap. The dispatch pipeline itself is unchanged — each message still returns a `MessageDisposition`.

## Transactional outbox

The optional `Runax.Messaging.Outbox` package makes "save data + publish" atomic. `AddOutbox()`
replaces `IMessagePublisher` with one that serializes and writes to an `IOutboxStore` instead of
publishing directly; a background `OutboxDispatcher` later drains pending rows to the transport and
marks them dispatched. A durable store's `AddAsync` enlists in the caller's database transaction, so
the message row commits together with the business data (at-least-once delivery — keep consumers
idempotent). See the [package README](../src/Runax.Messaging.Outbox/README.md).

## Design rules

- Transports depend on **Abstractions only**. Keep broker SDKs out of the core.
- One or more transports may be registered, each identified by a distinct `SystemName`. A consumer
  subscribes on every registered transport by default, or on a named subset; the hosted dispatcher
  subscribes and dispatches each transport independently. `IMessagePublisher` targets the sole
  transport, or the one chosen with `PublishTo("<system-name>")`.
- Applications depend on `IMessagePublisher` (from Abstractions), not on any
  concrete transport.
