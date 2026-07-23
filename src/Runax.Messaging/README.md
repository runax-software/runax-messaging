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

Versioning is opt-in. Tag a message type with `[MessageContract(version)]` and the version travels in the
envelope; consumers subscribe **one per version**, so several versions coexist on the same topic:

```csharp
[MessageContract(1)] public sealed record OrderV1(int Id, string Coupon);
[MessageContract(2)] public sealed record OrderV2(int Id, string Currency);

public sealed class OrderV1Consumer : MessageConsumer<OrderV1> { public override string Topic => "orders.placed"; }
public sealed class OrderV2Consumer : MessageConsumer<OrderV2> { public override string Topic => "orders.placed"; }
```

A `v1` message reaches only `OrderV1Consumer` (with full fidelity — its `Coupon` field intact); a `v2`
message reaches only `OrderV2Consumer`. A consumer with no `[MessageContract]` on its message type is
unversioned and receives every message on its topic (the pre-versioning behavior), so mixing is fine.

When a message arrives whose version **no** consumer handles, a pluggable strategy decides — it is never
silently dropped:

```csharp
messaging
    .AddRabbitMq(o => o.HostName = "localhost")
    .AddConsumer<OrderV2Consumer>()
    .OnUnroutableMessage(UnroutableStrategy.DeadLetter);   // default; or Requeue / Discard
```

For custom behavior (forward to a quarantine topic, alert, …) implement `IUnroutableMessageHandler` and
register it with `OnUnroutableMessage<MyHandler>()`; return the disposition the transport should apply.

Inject `IMessageContractCatalog` to check coverage at startup — e.g. `catalog.Accepts("orders.placed", 1)`
— before letting a producer emit a new version.

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
