# Configuration & per-broker settings

Runax.Messaging is configured inside a single `AddRunaxMessaging(runax => { ... })` block. Most
settings have a **global** form applied on the `runax` configurator, and several also have a
**per-broker** form applied inside a transport block (`AddRabbitMq(rabbitmq => { ... })`,
`AddInMemory(inMemory => { ... })`, and so on). A per-broker setting overrides the global one for
that broker only; every other broker keeps the global value (or the built-in default when no global
was set).

This page covers what can be set globally, what can be scoped per broker, and how the fallback
works. For the options each transport exposes (host, region, connection string, ...), see that
transport's package README.

## Setting reference

| Setting | Global? | Per-broker? | Fallback when not scoped |
| --- | --- | --- | --- |
| `AddConsumer<T>()` | Yes (subscribes on every transport) | Yes (subscribes on that broker only) | — |
| `WithRetry(o => ...)` | Yes | Yes | Global policy, else built-in `RetryOptions` defaults |
| `OnUnroutableMessage(strategy)` | Yes | Yes | Global strategy, else built-in `DeadLetter` |
| `OnUnroutableMessage<THandler>()` | Yes | Yes | Global handler, else built-in `DeadLetter` |
| `ConfigureSerialization(o => ...)` | Yes | Yes | Global JSON options, else defaults |
| `UseSerializer<T>()` | Yes | Yes | Global serializer, else `System.Text.Json` |
| `PublishTo("<system-name>")` | Yes | No (global-only) | Sole registered transport |
| Transport options (`Configure(o => ...)`) | No | Yes (belong to one broker) | — |

`PublishTo` is global-only by design: `IMessagePublisher` publishes to a single target, so choosing
that target is a global decision (a single registered transport is used automatically). Transport
options (`Configure`) are inherently per-broker — they describe one broker's connection.

## How scoping and fallback work

Every registered transport has a `SystemName` (`"rabbitmq"`, `"sqs"`, `"in-memory"`, ...). At publish
and consume time, Runax resolves each scoped setting **by that name**:

1. If the setting was configured inside that broker's transport block, the scoped value is used.
2. Otherwise the global value is used (the one set on `runax`).
3. Otherwise the built-in default applies.

So a per-broker call never affects other brokers, and omitting it simply falls back a level. This
mirrors how `AddConsumer<T>()` already works: inside a block it binds to that broker, at the top
level it binds to all of them.

## Retry policy

`WithRetry` tunes retry backoff, poison handling, and the dead-letter strategy (`RetryOptions`). Set
it globally, per broker, or both — the per-broker policy wins for its broker and the global policy
covers the rest:

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
        rabbitmq.WithRetry(o => o.MaxAttempts = 8);       // RabbitMQ: retry harder
    });

    runax.AddSqs(sqs =>
    {
        sqs.Configure(o => o.Region = "us-east-1");
        sqs.AddConsumer<OrderPlacedConsumer>();
        // no WithRetry here -> uses the global policy below
    });

    runax.WithRetry(o => o.MaxAttempts = 3);              // global default (SQS, and any other broker)
});
```

A scoped `RetryOptions` starts from the `RetryOptions` defaults with your action applied on top, and
is validated with the same DataAnnotations as the global policy.

## Unroutable-message strategy

`OnUnroutableMessage` decides the fate of a message no consumer accepts (an unhandled contract
version). Both forms — a built-in `UnroutableStrategy` and a custom `IUnroutableMessageHandler` —
can be global or per broker:

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<OrderV1Consumer>();
        rabbitmq.OnUnroutableMessage(UnroutableStrategy.Requeue);   // RabbitMQ: requeue
    });

    runax.AddSqs(sqs =>
    {
        sqs.Configure(o => o.Region = "us-east-1");
        sqs.AddConsumer<OrderV1Consumer>();
        sqs.OnUnroutableMessage<QuarantineUnroutableHandler>();     // SQS: custom handler
    });

    runax.OnUnroutableMessage(UnroutableStrategy.DeadLetter);       // global default (built-in default too)
});
```

## Serialization

`ConfigureSerialization` (tweak the `JsonSerializerOptions`) and `UseSerializer<T>()` (swap the body
serializer entirely) are global or per broker. A scoped `ConfigureSerialization` starts from a copy
of the global options and applies your action on top, so a broker inherits global settings and
overrides only what it needs. The framework-owned `__runax` envelope is identical regardless of the
serializer.

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
        rabbitmq.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase); // RabbitMQ only
    });

    runax.ConfigureSerialization(o => o.WriteIndented = false);   // global default
});
```

See [Serialization & custom serializers](serialization.md) for details.

## Choosing the publish target (global-only)

When more than one transport is registered, `PublishTo` selects which one `IMessagePublisher`
publishes to. It is a global setting — there is no per-broker form:

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<AuditConsumer>();
    });

    runax.AddSqs(sqs =>
    {
        sqs.Configure(o => o.Region = "us-east-1");
    });

    runax.PublishTo("sqs");   // IMessagePublisher publishes to SQS
});
```

## A note on style

Every configuration example in these docs uses the full nested block form — one statement per line
inside the `AddRunaxMessaging` and transport blocks — rather than a fluent chain. This keeps global
and per-broker settings visually distinct and easy to diff.

## See also

- [Architecture & message flow](architecture.md)
- [Serialization & custom serializers](serialization.md)
- Each transport's package README for its transport-specific options.
