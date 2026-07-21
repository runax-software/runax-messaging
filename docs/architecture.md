# Architecture & message flow

Runax.Messaging separates the messaging *contract* from its *implementation* and
from each *transport*, so applications depend on stable abstractions while
brokers stay swappable.

## Package layering

```
Runax.Messaging.Abstractions      (contracts: IMessagePublisher, IMessagingTransport, MessageContext, MessagingConfigurator)
        ▲                 ▲
        │                 │
Runax.Messaging      Runax.Messaging.Sqs / .RabbitMq / <your transport>
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
| `MessagingConfigurator` | Abstractions | Fluent builder; transports and consumers attach via extensions. |
| `RetryOptions` / `DeadLetterStrategy` | Core | Retry backoff and dead-letter policy applied by the dispatcher (`WithRetry`). |
| `MessagingDiagnostics` | Core | The `ActivitySource` and `Meter` names for tracing and metrics. |
| `MessagePublisherAdapter` | Core (internal) | Bridges `IMessagePublisher` → `IMessagingTransport`, serializing to an envelope and emitting publish telemetry. |
| `IMessageSerializer` / `JsonMessageSerializer` | Core (internal) | Envelope serialization (System.Text.Json). |
| `MessageConsumer<TMessage>` | Core | Base class for a typed consumer of a single topic. |
| `MessageConsumerHostedService` | Core (internal) | Background service that subscribes consumers and dispatches messages with retry, dead-lettering, and telemetry. |

## The envelope

Messages travel wrapped in an envelope so metadata rides alongside the payload:

```json
{
  "MessageType": "MyApp.Order, MyApp",
  "Body": "{\"Id\":1,\"Name\":\"widget\"}",
  "Headers": { "correlation-id": "abc" }
}
```

`Body` is the JSON-serialized message; `Headers` carries transport-level
metadata. The transport only ever sees the serialized envelope string — it never
needs to know your message types.

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
                        ├─ MessageConsumer<T>: deserialize Body → T → HandleAsync(T)
                        │       └─ on failure: retry with backoff, then dead-letter (see below)
                        └─ return Acknowledge / Requeue / DeadLetter to the transport
```

Consumers are ordinary DI singletons. Dispatch runs inside a hosted
`BackgroundService`, so consuming requires a .NET Generic Host. Publishing has no
such requirement.

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
- Exactly one transport is registered per configuration.
- Applications depend on `IMessagePublisher` (from Abstractions), not on any
  concrete transport.
