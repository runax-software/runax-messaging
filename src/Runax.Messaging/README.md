# Runax.Messaging

The default implementation for [Runax.Messaging](https://github.com/runax-software/runax-messaging):
dependency-injection wiring (`AddBus` / `BusBuilder`), JSON serialization, hosted message
consumers, and a built-in in-memory transport. Add a transport package (SQS, RabbitMQ) for
cross-process delivery, or use the in-memory transport for tests and
single-process apps.

## Install

```bash
dotnet add package Runax.Messaging
```

## Register

Configuration is organized around **buses** — a bus is a named messaging context wrapping
exactly one transport, plus its consumers and policies:

```csharp
using Runax.Messaging;
using Runax.Messaging.InMemory;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new InMemoryConfig());
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddRunaxMessaging` registers the default serializer and the `IBus` resolution, then invokes
your configuration. Register at least one bus, each with exactly one transport. If a bus has
consumers, a hosted background service is registered per bus to dispatch them — so consuming
requires a .NET Generic Host.

## Publish

```csharp
using Runax.Messaging.Abstractions;

public sealed class Checkout(IBus bus)
{
    public ValueTask PlaceOrderAsync(Order order) =>
        bus.PublishAsync("orders.placed", order);
}
```

An unkeyed `IBus` resolves the default bus, so single-bus apps never touch bus names. Publish
many at once with `PublishBatchAsync(topic, messages)`, which uses the transport's batch API
where available (SQS `SendMessageBatch`, a single RabbitMQ confirm per batch).

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

## Multiple buses

A bus wraps exactly one transport — a second `AddTransport` on the same bus throws at
configuration time — so talking to several brokers (or two clusters of the same broker) means
registering several buses:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>                          // default bus: unkeyed IBus resolves it
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<AuditConsumer>();
        bus.AddConsumer<OrderPlacedConsumer>();
    });

    messaging.AddBus("audit", bus =>                 // named bus, keyed resolution
    {
        bus.Mode = BusMode.PublishOnly;              // optional: registering a consumer here throws
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
    });
});
```

Each bus is fully isolated: its own transport connection, consumers, retry policy, serializers,
health check (`runax:{bus}`), and hosted lifecycle. Register the same consumer type on two
buses to consume its topic from both brokers (it stays a single instance; each bus's traffic
flows through that bus's own pipeline). Named buses resolve via keyed DI
(`[FromKeyedServices("audit")] IBus`) or `IBusProvider.GetBus("audit")`.

To publish the same event to **several** brokers, inject each bus and publish on both:

```csharp
public sealed class OrderService(
    IBus main,
    [FromKeyedServices("audit")] IBus audit)
{
    public async Task PlaceAsync(OrderPlaced order, CancellationToken ct)
    {
        await main.PublishAsync("orders", order, ct);
        await audit.PublishAsync("orders", order, ct);
    }
}
```

The two sends are independent (no atomic fan-out).

## Retries & dead-lettering

Failed `HandleAsync` calls are retried with exponential backoff, and messages that
cannot be handled are dead-lettered. Tune the policy with `bus.WithRetry`:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new InMemoryConfig());
        bus.AddConsumer<OrderPlacedConsumer>();

        bus.WithRetry(o =>
        {
            o.MaxAttempts = 5;                 // initial attempt + retries
            o.InitialDelay = TimeSpan.FromMilliseconds(200);
            o.BackoffFactor = 2.0;
            o.MaxDelay = TimeSpan.FromSeconds(30);
            // o.Strategy = DeadLetterStrategy.BrokerNative; // defer to broker DLX / redrive
        });
    });
});
```

- **Retry** — up to `MaxAttempts`, growing by `BackoffFactor` up to `MaxDelay`.
- **Poison messages** — throw `PoisonMessageException` from a consumer to skip
  retries and dead-letter immediately.
- **Dead-letter strategy** — `FrameworkManaged` (default) republishes to
  `{topic}.dead-letter` with `x-runax-dlq-*` headers; `BrokerNative` rejects the
  message so the transport's native DLQ handles it (pair with `MaxAttempts = 1` to
  rely purely on the broker).

`WithRetry` is **per bus** — each bus sets its own policy, falling back to the built-in
defaults. Override it for a single topic with `WithRetryForTopic` (the most specific scope
wins):

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 8);                        // this bus's policy
        bus.WithRetryForTopic("payments", o => o.MaxAttempts = 10);   // "payments" only
    });
});
```

## Serialization

Message bodies are serialized with `System.Text.Json`. Configure the options — naming policy,
converters, or a source-generated `JsonSerializerContext` (via `TypeInfoResolver`) for a
trim-friendly / AOT path — with `bus.ConfigureSerialization`:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new InMemoryConfig());
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
    });
});
```

The same options are applied on both publish and consume; they start from a copy of the
container's global `JsonSerializerOptions` with your action applied on top. To swap the body
serializer entirely, implement `ISerializer` and register it with `bus.UseSerializer<T>()`.
Both are **per bus**, with `ConfigureSerializationForTopic` / `UseSerializerForTopic<T>` for
topic-level overrides; the framework-owned `__runax` envelope is identical either way. See
[Serialization & custom serializers](../../docs/serialization.md).

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

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderV1Consumer>();
        bus.AddConsumer<OrderV2Consumer>();
    });
});
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
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderV1Consumer>();
        bus.OnUnroutableMessage(UnroutableStrategy.DeadLetter);   // this is the default
    });
});
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

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderV1Consumer>();
        bus.OnUnroutableMessage<AlertingUnroutableHandler>();
    });
});
```

Like `WithRetry`, `OnUnroutableMessage` (both the strategy and the custom-handler form) is
**per bus** — each bus picks its own strategy, falling back to the built-in dead-letter
default. See [Configuration & per-bus settings](../../docs/configuration.md).

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
  conventions plus `messaging.runax.bus` (the bus name).
- **Metrics** — `runax.messaging.published` / `consumed` / `failed` counters and a
  `runax.messaging.processing.duration` histogram, tagged by system, destination, and bus.

Each bus with a broker transport auto-registers a health check named `runax:{bus}`
(opt out with `RegisterHealthCheck = false` on the transport config).

## In-memory transport

`bus.AddTransport(new InMemoryConfig())` delivers messages in-process through channels, one
per topic. It is intended for tests and single-process scenarios — messages are not persisted
and do not cross process boundaries.

The in-memory config has **no transport settings** of its own. The bus's core settings apply
as usual — scope consumers or override policy on the bus that runs it:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new InMemoryConfig());
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 1);   // this bus's policy
    });
});
```

Its `SystemName` is `in-memory`, and it registers no health check. See
[Configuration & per-bus settings](../../docs/configuration.md).

## License

MIT
