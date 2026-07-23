# Runax.Messaging

The default implementation for [Runax.Messaging](https://github.com/runax-software/runax-messaging):
dependency-injection wiring, JSON serialization, hosted message consumers, and a
built-in in-memory transport. Add a transport package (SQS, RabbitMQ) for
cross-process delivery, or use the in-memory transport for tests and
single-process apps.

## Install

```bash
dotnet add package Runax.Messaging
```

## Register

```csharp
using Runax.Messaging;

builder.Services.AddRunaxMessaging(messaging => messaging
    .AddInMemory()
    .AddConsumer<OrderPlacedConsumer>());
```

`AddRunaxMessaging` registers the JSON serializer and `IMessagePublisher`, then
invokes your configuration. Register one or more transports. If any consumers are
added, a hosted background service is registered to dispatch them — so consuming
requires a .NET Generic Host.

## Publish

```csharp
using Runax.Messaging.Abstractions;

public sealed class Checkout(IMessagePublisher publisher)
{
    public ValueTask PlaceOrderAsync(Order order) =>
        publisher.PublishAsync("orders.placed", order);
}
```

Publish many at once with `PublishBatchAsync(topic, messages)`, which uses the transport's
batch API where available (SQS `SendMessageBatch`, a single RabbitMQ confirm per batch).

## Consume

Derive from `MessageConsumer<TMessage>`; the framework deserializes the body
before calling `HandleAsync`:

```csharp
using Runax.Messaging;

public sealed class OrderPlacedConsumer : MessageConsumer<Order>
{
    public override string Topic => "orders.placed";

    protected override ValueTask HandleAsync(Order order, CancellationToken cancellationToken)
    {
        // handle the message
        return ValueTask.CompletedTask;
    }
}
```

## Multiple transports

Register several transports (each identified by its `SystemName`) and a single consumer can receive its
topic from more than one broker — even different ones:

```csharp
builder.Services.AddRunaxMessaging(messaging => messaging
    .AddRabbitMq(o => o.HostName = "localhost")
    .AddSqs(o => o.Region = "us-east-1")
    .AddConsumer<OrderPlacedConsumer>()            // every registered transport
    .AddConsumer<AuditConsumer>("rabbitmq")        // only RabbitMQ
    .PublishTo("sqs"));                            // IMessagePublisher target
```

`AddConsumer<T>()` subscribes on all registered transports; pass one or more `SystemName`s to target
specific brokers. Each transport is subscribed and dispatched independently. With more than one transport
registered, `PublishTo("<system-name>")` selects the publish target (a single transport is used
automatically). Each transport must report a distinct `SystemName`.

## Retries & dead-lettering

Failed `HandleAsync` calls are retried with exponential backoff, and messages that
cannot be handled are dead-lettered. Tune the policy with `WithRetry`:

```csharp
builder.Services.AddRunaxMessaging(messaging => messaging
    .AddInMemory()
    .AddConsumer<OrderPlacedConsumer>()
    .WithRetry(o =>
    {
        o.MaxAttempts = 5;                 // initial attempt + retries
        o.InitialDelay = TimeSpan.FromMilliseconds(200);
        o.BackoffFactor = 2.0;
        o.MaxDelay = TimeSpan.FromSeconds(30);
        // o.Strategy = DeadLetterStrategy.BrokerNative; // defer to broker DLX / redrive
    }));
```

- **Retry** — up to `MaxAttempts`, growing by `BackoffFactor` up to `MaxDelay`.
- **Poison messages** — throw `PoisonMessageException` from a consumer to skip
  retries and dead-letter immediately.
- **Dead-letter strategy** — `FrameworkManaged` (default) republishes to
  `{topic}.dead-letter` with `x-runax-dlq-*` headers; `BrokerNative` rejects the
  message so the transport's native DLQ handles it (pair with `MaxAttempts = 1` to
  rely purely on the broker).

## Serialization

Message bodies are serialized with `System.Text.Json`. Configure the options — naming policy,
converters, or a source-generated `JsonSerializerContext` (via `TypeInfoResolver`) for a
trim-friendly / AOT path — with `ConfigureSerialization`:

```csharp
builder.Services.AddRunaxMessaging(messaging => messaging
    .AddInMemory()
    .ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase));
```

The same options are applied on both publish and consume.

## Contract versioning

Message shapes change over time — you add a field, rename one, drop another. Versioning lets the old and new
shapes flow over the same topic while old and new consumers run side by side, so you can roll a change out
without a coordinated big-bang deploy. It's **opt-in**: add nothing and every consumer keeps receiving every
message on its topic, exactly as before.

### 1. Put a version on the message type

The publisher reads it and stamps it into the envelope for you — your publishing code doesn't change.

```csharp
[MessageContract(1)] public sealed record OrderV1(int Id, string Coupon);
[MessageContract(2)] public sealed record OrderV2(int Id, string Currency);   // dropped Coupon, added Currency
```

### 2. Write one consumer per version

Both subscribe to the same topic:

```csharp
public sealed class OrderV1Consumer : MessageConsumer<OrderV1>
{
    public override string Topic => "orders.placed";
    protected override ValueTask HandleAsync(OrderV1 order, CancellationToken ct) { /* ... */ }
}

public sealed class OrderV2Consumer : MessageConsumer<OrderV2>
{
    public override string Topic => "orders.placed";
    protected override ValueTask HandleAsync(OrderV2 order, CancellationToken ct) { /* ... */ }
}

messaging
    .AddRabbitMq(o => o.HostName = "localhost")
    .AddConsumer<OrderV1Consumer>()
    .AddConsumer<OrderV2Consumer>();
```

Runax routes each message to the consumer for **its** version: a `v1` message goes only to `OrderV1Consumer`,
a `v2` message only to `OrderV2Consumer`. Each consumer receives its own exact type, so `OrderV1Consumer` still
sees the `Coupon` field that `v2` removed — nothing is lost in translation.

> **No attribute = unversioned.** A consumer whose message type has no `[MessageContract]` receives *every*
> message on its topic, whatever the version. That's the original behaviour and your escape hatch when you
> don't want per-version routing.

### Rolling out a new version

1. Deploy your app with **both** `OrderV1Consumer` and `OrderV2Consumer`. It now handles either version.
2. Switch the producer to publish `OrderV2`. New messages go to the v2 consumer; any in-flight `v1` messages
   still go to the v1 consumer.
3. When no `v1` messages remain, delete `OrderV1Consumer` and `OrderV1`.

The rule of thumb: **deploy consumers before the producer starts sending the new version.** The next section
covers what happens if a version shows up that you don't handle yet.

### What happens to a version nobody handles

If a message arrives whose version no consumer accepts — e.g. a `v2` order reaches an app that still only has
the `v1` consumer — it is **never silently dropped**. You choose the outcome:

```csharp
messaging
    .AddConsumer<OrderV1Consumer>()
    .OnUnroutableMessage(UnroutableStrategy.DeadLetter);   // this is the default
```

| Strategy | What it does |
| --- | --- |
| `DeadLetter` *(default)* | Dead-letters it through your `DeadLetterStrategy` (a `{topic}.dead-letter` topic, or the broker's native DLQ). Redrive it once the missing consumer ships — nothing is lost. |
| `Requeue` | Puts it back for redelivery. Only safe when the consumer is about to appear — otherwise it loops forever. |
| `Discard` | Acknowledges and drops it. |

Need something else — forward to a quarantine topic, page on-call, log and move on? Implement
`IUnroutableMessageHandler` and return the disposition the transport should apply:

```csharp
public sealed class AlertingUnroutableHandler(ILogger<AlertingUnroutableHandler> logger) : IUnroutableMessageHandler
{
    public ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken ct)
    {
        logger.LogWarning("Unhandled contract version {Version} on '{Topic}'", message.ContractVersion, message.Topic);
        return ValueTask.FromResult(MessageDisposition.DeadLetter);   // then still dead-letter it
    }
}

messaging.OnUnroutableMessage<AlertingUnroutableHandler>();
```

### Check what you handle before switching a producer

`IMessageContractCatalog` reports the `(topic, version)` pairs your app consumes, so you can fail fast if a
consumer is missing:

```csharp
var catalog = host.Services.GetRequiredService<IMessageContractCatalog>();
if (!catalog.Accepts("orders.placed", 2))
    throw new InvalidOperationException("Deploy the v2 consumer before publishing v2 orders.");
```

## Observability

Publish and consume are instrumented with the in-box `System.Diagnostics`
primitives — no OpenTelemetry SDK dependency. Subscribe with the names on
`MessagingDiagnostics` (both `"Runax.Messaging"`):

```csharp
tracerProviderBuilder.AddSource(MessagingDiagnostics.ActivitySourceName);
meterProviderBuilder.AddMeter(MessagingDiagnostics.MeterName);
```

- **Spans** — a producer span on publish (W3C context injected into the envelope
  headers) and a consumer span on consume, tagged per OpenTelemetry messaging
  conventions.
- **Metrics** — `runax.messaging.published` / `consumed` / `failed` counters and a
  `runax.messaging.processing.duration` histogram.

Transport packages add broker health checks (`AddRabbitMqTransport()`,
`AddSqsTransport()`).

## In-memory transport

`AddInMemory()` delivers messages in-process through channels, one per topic. It
is intended for tests and single-process scenarios — messages are not persisted
and do not cross process boundaries.

## License

MIT
