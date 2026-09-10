# Configuration & per-bus settings

Runax.Messaging is configured inside a single `AddRunaxMessaging(messaging => { ... })` block.
Everything hangs off **buses**: `messaging.AddBus(bus => { ... })` adds the default bus and
`messaging.AddBus("name", bus => { ... })` adds a named one. Each bus wraps **exactly one
transport** (registered with `bus.AddTransport(...)`) plus its own consumers, retry policy,
serialization, and unroutable-message handling. Several settings also have a **per-topic** form
applied on the same bus; a per-topic setting overrides the bus-wide one for that topic only.

This page covers what can be set on a bus, what can be scoped per topic, and how the fallback
works. For the config properties each transport exposes (host, region, connection string, ...),
see that transport's package README.

## Setting reference

All of these are methods on the `BusBuilder` inside an `AddBus` block:

| Setting | Bus-wide? | Per-topic? | Fallback when not scoped |
| --- | --- | --- | --- |
| `AddTransport(config)` / `AddTransport<TConfig>(...)` | Yes (exactly one per bus) | — | — (required; zero or two transports throw at configuration time) |
| `AddConsumer<T>()` | Yes (subscribes on this bus's transport) | — | — |
| `Mode = BusMode.PublishOnly / ConsumeOnly` | Yes | — | `PublishAndConsume` |
| `WithRetry(o => ...)` | Yes | — | Built-in `RetryOptions` defaults |
| `WithRetryForTopic("<topic>", o => ...)` | — | Yes | Bus policy, then defaults |
| `OnUnroutableMessage(strategy)` | Yes | No | Built-in `DeadLetter` |
| `OnUnroutableMessage<THandler>()` | Yes | No | Built-in `DeadLetter` |
| `ConfigureSerialization(o => ...)` | Yes | — | Container-wide JSON options, else defaults |
| `UseSerializer<T>()` | Yes | — | `System.Text.Json` |
| `ConfigureSerializationForTopic("<topic>", o => ...)` | — | Yes | Bus serializer, then default |
| `UseSerializerForTopic<T>("<topic>")` | — | Yes | Bus serializer, then default |

There is deliberately **no global scope**: nothing configured on one bus leaks into another. An
application that wants shared defaults across buses writes a helper and applies it to each:

```csharp
Action<BusBuilder> defaults = bus => bus.WithRetry(o => o.MaxAttempts = 3);

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus => { bus.AddTransport(new RabbitMqConfig()); defaults(bus); });
    messaging.AddBus("audit", bus => { bus.AddTransport(new SqsConfig()); defaults(bus); });
});
```

## How scoping and fallback work

Settings resolve **by bus name** (the identity application code uses; the broker's `SystemName`
is telemetry only). At publish and consume time, Runax resolves each scoped setting
most-specific-first:

1. If the setting was configured for that topic on that bus (`*ForTopic`), the topic value is used.
2. Otherwise the bus-wide value is used (the one set on the `BusBuilder`).
3. Otherwise the built-in default applies.

Two buses never share policy state — that is the point of having buses. Registering the same
consumer type on two buses delivers each bus's traffic through that bus's own pipeline.

## Registering the transport

`AddTransport` accepts a pre-built config instance, a delegate, or an `IConfiguration` section
to bind:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });          // instance
    });

    messaging.AddBus("events", bus =>
    {
        bus.AddTransport<KafkaConfig>(c => c.BootstrapServers = "localhost:9092"); // delegate
    });

    messaging.AddBus("audit", bus =>
    {
        bus.AddTransport<SqsConfig>(builder.Configuration.GetSection("Messaging:AuditSqs")); // bound
    });
});
```

Configs are validated with their DataAnnotations when the `AddBus` block completes, so a
misconfigured bus fails at startup with an `InvalidOperationException` naming the bus. A second
`AddTransport` on the same bus throws — a bus wraps exactly one transport; register an
additional bus for an additional broker.

## Bus modes

A bus can declare its relationship with the broker up front via `bus.Mode` (default
`PublishAndConsume`):

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus("partner-feed", bus =>
    {
        bus.Mode = BusMode.ConsumeOnly;    // publishing on this bus throws at runtime
        bus.AddTransport<KafkaConfig>(builder.Configuration.GetSection("Messaging:PartnerKafka"));
        bus.AddConsumer<PartnerEventConsumer>();
    });

    messaging.AddBus("audit", bus =>
    {
        bus.Mode = BusMode.PublishOnly;    // AddConsumer / consume-side policies here throw at configuration time
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
    });
});
```

- `PublishOnly` — consumer registrations (and consume-side policies like `WithRetry`) throw when
  the `AddBus` block completes, and no consumer hosted service is started for the bus.
- `ConsumeOnly` — `PublishAsync` / `PublishBatchAsync` on the bus throw at runtime, and
  configuring an outbox throws at configuration time. The consume pipeline's own dead-letter
  publish still works (it is part of consuming); if your broker credentials genuinely cannot
  write, use `DeadLetterStrategy.BrokerNative` or disable dead-lettering on that bus.

Beyond guardrails, the mode drives resource allocation: transports see it via
`TransportContext.Mode` and skip building the unused side (no publish channel pool on a
`ConsumeOnly` RabbitMQ bus, no subscription resources on a `PublishOnly` bus).

## Retry policy

`WithRetry` tunes retry backoff, poison handling, and the dead-letter strategy (`RetryOptions`)
for this bus's consumers. Each bus sets its own policy; a bus without one uses the built-in
defaults:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 8);       // this bus: retry harder
    });

    messaging.AddBus("audit", bus =>
    {
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
        bus.AddConsumer<OrderPlacedConsumer>();
        // no WithRetry here -> built-in RetryOptions defaults
    });
});
```

A scoped `RetryOptions` starts from the `RetryOptions` defaults with your action applied on top,
and is validated with the same DataAnnotations as any other policy.

Retry can also be scoped **per topic** with `WithRetryForTopic("<topic>", o => ...)` — the most
specific scope, winning over the bus-wide policy for that topic. This is useful when a topic's
semantics, not its broker, decide how hard to retry: a `payments` command wants more attempts
than a `telemetry` stream:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport<KafkaConfig>(c => c.BootstrapServers = "localhost:9092");
        bus.AddConsumer<PaymentConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 5);                        // bus default for any topic
        bus.WithRetryForTopic("payments", o => o.MaxAttempts = 10);   // "payments" on this bus only
        bus.WithRetryForTopic("telemetry", o => o.MaxAttempts = 1);
    });
});
```

Like the bus policy, a per-topic policy starts from the `RetryOptions` defaults with your action
applied on top.

Retry is a **consumer-side** policy: it governs how a failing `HandleAsync` is retried and
dead-lettered, so per-topic retry keys off the topic a consumer is subscribed to. Publishing has
no retry loop of its own.

## Unroutable-message strategy

`OnUnroutableMessage` decides the fate of a message no consumer accepts (an unhandled contract
version). Both forms — a built-in `UnroutableStrategy` and a custom `IUnroutableMessageHandler`
— are bus-scoped:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderV1Consumer>();
        bus.OnUnroutableMessage(UnroutableStrategy.Requeue);       // this bus: requeue
    });

    messaging.AddBus("audit", bus =>
    {
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
        bus.AddConsumer<OrderV1Consumer>();
        bus.OnUnroutableMessage<QuarantineUnroutableHandler>();    // this bus: custom handler
    });

    // any bus without OnUnroutableMessage uses the built-in DeadLetter default
});
```

## Serialization

`ConfigureSerialization` (tweak the `JsonSerializerOptions`) and `UseSerializer<T>()` (swap the
body serializer entirely) are bus-scoped, with `ConfigureSerializationForTopic` /
`UseSerializerForTopic<T>` for topic-level overrides. A bus's `ConfigureSerialization` starts
from a copy of the container's global `JsonSerializerOptions` and applies your action on top, so
the bus inherits application-wide settings and overrides only what it needs. The framework-owned
`__runax` envelope is identical regardless of the serializer.

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase); // this bus only
    });
});
```

See [Serialization & custom serializers](serialization.md) for details.

## Choosing the bus to publish on

There is no publish-target setting: **a bus always publishes to its only transport**. Which bus
you publish on is decided at the injection site:

- An **unkeyed** `IBus` resolves the default bus (the parameterless `AddBus`). If no default bus
  exists but exactly one named bus does, that bus is the unkeyed target; with several named
  buses and no default, unkeyed resolution throws with the registered bus names listed. A
  single-bus app therefore never touches bus names.
- A **named** bus resolves via keyed DI — `[FromKeyedServices("audit")] IBus` — or via
  `IBusProvider.GetBus("audit")` for dynamic lookup.

```csharp
public sealed class OrderService(IBus bus)                              // default bus
{
    public ValueTask PlaceAsync(OrderPlaced evt, CancellationToken ct) =>
        bus.PublishAsync("orders.placed", evt, ct);
}

public sealed class AuditTrail([FromKeyedServices("audit")] IBus audit) // named bus
{
    public ValueTask RecordAsync(AuditEntry entry, CancellationToken ct) =>
        audit.PublishAsync("audit.entry", entry, ct);
}
```

## Publishing on several buses

To send the same event to more than one broker, inject each bus and publish on both — cross-bus
flows are always explicit; there is no implicit routing, mirroring, or fallback between buses:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport<KafkaConfig>(c => c.BootstrapServers = "localhost:9092");
    });

    messaging.AddBus("audit", bus =>
    {
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
    });
});
```

```csharp
public sealed class OrderService(
    IBus main,
    [FromKeyedServices("audit")] IBus audit)
{
    public async Task PlaceAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        await main.PublishAsync("orders", order, cancellationToken);
        await audit.PublishAsync("orders", order, cancellationToken);
    }
}
```

Notes:

- The two publishes are independent operations — there is no built-in atomic fan-out. If sending
  on the second bus must not be lost when the first succeeds, give each bus its own outbox or
  add your own coordination.
- `IBusProvider.GetBus("<name>")` throws for an unknown bus name, so typos fail fast;
  `IBusProvider.Buses` enumerates every configured bus for diagnostics and admin surfaces.
- A single unkeyed `IBus` still works unchanged for the common one-bus case; you only reach for
  keyed injection (or the provider) when you have more than one bus.

## A note on style

Every configuration example in these docs uses the full nested block form — one statement per
line inside the `AddRunaxMessaging` and `AddBus` blocks — rather than a fluent chain. This keeps
bus-wide and per-topic settings visually distinct and easy to diff.

## See also

- [Architecture & message flow](architecture.md)
- [Serialization & custom serializers](serialization.md)
- [Migrating from 1.x to 2.0](migrating-to-2.0.md)
- Each transport's package README for its transport-specific config properties.
