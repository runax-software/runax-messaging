# Buses

The **bus** is the organizing concept of Runax.Messaging 2.0. A bus is a named,
self-contained messaging context: exactly one transport, plus the consumers, mode, and
policies (retry, serialization, unroutable-message handling, optionally an outbox) that
belong to it. Everything else in the library — publishing, consumer dispatch, health
checks, telemetry — hangs off a bus. This page is the deep-dive: what a bus is, how to
declare and resolve one, what bus modes enforce, and the patterns that fall out of
running several buses side by side.

If you are coming from 1.x, see [Migrating from 1.x to 2.0](migrating-to-2.0.md) for how
the old publisher/transport registration maps onto buses.

## What a bus is

A bus is the single application handle to one broker. Application code depends on `IBus`
(from `Runax.Messaging.Abstractions`) and never on a concrete transport:

```csharp
public interface IBus
{
    string Name { get; }
    BusMode Mode { get; }

    ValueTask PublishAsync<TMessage>(string topic, TMessage message, CancellationToken cancellationToken = default);
    ValueTask PublishAsync<TMessage>(string topic, TMessage message, IDictionary<string, string> headers, CancellationToken cancellationToken = default);
    ValueTask PublishBatchAsync<TMessage>(string topic, IReadOnlyList<TMessage> messages, CancellationToken cancellationToken = default);
}
```

### One bus = exactly one transport

A bus wraps **exactly one** transport, enforced at configuration time. This is not an
arbitrary restriction — it is what makes the bus name a complete identity. Because a bus
can only ever talk to one broker, everything collapses onto the bus name:

- **Publishing.** `bus.PublishAsync(topic, ...)` needs no target selection — a bus always
  publishes to its only transport. There is no routing table and no publish-target setting.
- **Consumer binding.** `bus.AddConsumer<T>()` subscribes the consumer on this bus's
  transport; the per-bus dispatcher delivers this bus's traffic to it.
- **Policies.** Retry, serialization, and unroutable-message settings are keyed by bus
  name (with per-topic overrides on the same bus). See
  [Configuration & per-bus settings](configuration.md).
- **Health checks.** Each bus auto-registers a broker-reachability check named
  `runax:{bus}` (opt out with `RegisterHealthCheck = false` on the transport config).
- **Telemetry.** Publish and consume spans and every metric carry the
  `messaging.runax.bus` tag — the tag that disambiguates two buses on one dashboard.
- **Named options.** Scoped policies such as `WithRetry` register named
  `RetryOptions` under the bus name, validated with DataAnnotations at startup.

The transport's `SystemName` (`"rabbitmq"`, `"sqs"`, ...) is deliberately **not** an
identity — it is only the OpenTelemetry `messaging.system` tag. Any number of buses may
use the same system, which is how two clusters of the same broker are expressed (see
[Multi-bus patterns](#multi-bus-patterns)).

The consequence to internalize: **multiple brokers = multiple buses**. Talking to a
RabbitMQ cluster and an SQS account is two buses; talking to two RabbitMQ clusters is
also two buses. Each bus gets its own connection, its own config validation, its own
health check, its own policies, and its own hosted consumer lifecycle.

## Declaring buses

Buses are declared inside `AddRunaxMessaging`, one `AddBus` block per bus:

- `messaging.AddBus(bus => { ... })` adds the **default bus**, registered under the
  well-known name `BusNames.Default` (`"default"`). This is what an unkeyed `IBus`
  injection resolves to, so single-bus applications never touch bus names.
- `messaging.AddBus("audit", bus => { ... })` adds a **named bus**, resolvable via keyed
  DI or `IBusProvider`. Names must be unique per application.

### The three `AddTransport` styles

Inside the block, the bus's one transport is registered with `AddTransport`, in whichever
of its three forms fits:

| Form | Use when |
| --- | --- |
| `bus.AddTransport(config)` | You have a pre-built `TransportConfig` instance (settings known in code, or built by your own factory). |
| `bus.AddTransport<TConfig>(c => ...)` | You want the config created for you and set a few properties inline. |
| `bus.AddTransport<TConfig>(section)` | The settings live in `IConfiguration` — the section is bound onto a new config instance. |

### A full example

An order-processing service with three buses: the default bus on the team's RabbitMQ
cluster (publish and consume), a publish-only `audit` bus on SQS, and a consume-only
`partner-feed` bus on a partner's Kafka cluster:

```csharp
// Program.cs
using Runax.Messaging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Aws.Sqs;
using Runax.Messaging.Transports.Kafka;
using Runax.Messaging.Transports.RabbitMq;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>                                     // the default bus
    {
        bus.AddTransport(new RabbitMqConfig
        {
            HostName = "rabbit.orders.internal",
            UserName = "orders-svc",
            Password = builder.Configuration["Rabbit:Password"]!,
        });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 5);
    });

    messaging.AddBus("audit", bus =>                            // named, publish-only
    {
        bus.Mode = BusMode.PublishOnly;
        bus.AddTransport<SqsConfig>(c => c.Region = "eu-west-1");
    });

    messaging.AddBus("partner-feed", bus =>                     // named, consume-only
    {
        bus.Mode = BusMode.ConsumeOnly;
        bus.AddTransport<KafkaConfig>(builder.Configuration.GetSection("Messaging:PartnerKafka"));
        bus.AddConsumer<PartnerPriceUpdateConsumer>();
    });
});

var app = builder.Build();
app.Run();
```

```json
// appsettings.json
{
  "Messaging": {
    "PartnerKafka": {
      "BootstrapServers": "broker1.partner.example:9092,broker2.partner.example:9092",
      "ConsumerGroupId": "orders-service",
      "SecurityProtocol": "SaslSsl",
      "SaslMechanism": "Plain",
      "SaslUsername": "orders-app"
    }
  }
}
```

### What happens when the block completes

The `AddBus` block is not just registration sugar — when your `Action<BusBuilder>`
returns, the bus is **validated**, so a misconfigured bus fails at startup with an
`InvalidOperationException` naming the bus, before the host runs:

1. **Unique name.** A duplicate bus name throws immediately:
   `A bus named 'audit' is already registered. Bus names must be unique; pick a different name for the additional bus.`
2. **Exactly one transport.** Zero transports throw at end of block:
   `Bus 'audit' has no transport. Register exactly one with bus.AddTransport(...) (e.g. bus.AddTransport(new InMemoryConfig())).`
   A second `AddTransport` throws at the call itself:
   `Bus 'audit' already has a transport ('sqs'). A bus wraps exactly one transport — register an additional bus (messaging.AddBus("<name>", ...)) for an additional broker.`
3. **Mode rules.** Registrations incompatible with `bus.Mode` throw at end of block —
   see [Bus modes](#bus-modes) for the full table.
4. **DataAnnotations on the transport config.** The config's `[Required]`, `[Range]`,
   etc. are validated; failures throw with every violation listed:
   `Bus 'partner-feed': the KafkaConfig transport config is invalid: The BootstrapServers field is required.`
5. **Extension validations.** Callbacks registered via `BusBuilder.OnValidate` run last —
   extension packages (e.g. the outbox) use this to enforce their own pairing rules.

Only after validation passes does `AddBus` register the bus: a keyed
`IMessagingTransport` and `IBus` singleton under the bus name, the transport's own
service registrations (health check, broker SDK clients), and — if the bus registered any
consumers — one `MessageConsumerHostedService` for the bus.

## Resolving buses

There are three ways to get hold of a bus. Pick the least dynamic one that works.

### Unkeyed `IBus` — the common case

An unkeyed `IBus` injection resolves by these rules, in order:

1. If a **default bus** exists (the parameterless `AddBus`), that is the unkeyed bus.
2. Otherwise, if **exactly one named bus** exists, that sole bus is the unkeyed target —
   a one-bus app never references its own bus name, even if the bus happens to be named.
3. Otherwise resolution throws:
   `Several named buses are registered and none is the default, so an unkeyed IBus is ambiguous. Inject a keyed bus ([FromKeyedServices("<name>")] IBus) or use IBusProvider.GetBus(...). Registered buses: audit, partner-feed.`

(With no bus registered at all, resolving `IBus` throws
`No bus is registered. Add one inside AddRunaxMessaging (e.g. messaging.AddBus(bus => bus.AddTransport(new InMemoryConfig()))) before resolving IBus.`)

### Keyed `IBus` — a specific named bus

Every bus is a keyed singleton under its name:

```csharp
public sealed class OrderService(
    IBus bus,                                   // the default bus
    [FromKeyedServices("audit")] IBus audit)    // the "audit" bus
{
    public async Task PlaceAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        await bus.PublishAsync("orders.placed", order, cancellationToken);
        await audit.PublishAsync("audit.order-placed", order, cancellationToken);
    }
}
```

### `IBusProvider` — dynamic lookup and enumeration

`IBusProvider` resolves buses by name at runtime and enumerates all of them:

```csharp
public interface IBusProvider
{
    IBus GetBus(string name);          // throws for an unknown name
    IReadOnlyList<IBus> Buses { get; } // every configured bus, in registration order
}
```

`GetBus` fails fast on typos:
`No bus is registered under the name 'audot'. Registered buses: default, audit, partner-feed.`

### Which to use where

| Situation | Use |
| --- | --- |
| Ordinary application code, one bus in play | Unkeyed `IBus` |
| Code that targets a specific named bus | `[FromKeyedServices("<name>")] IBus` — the dependency is visible in the constructor signature |
| Bus chosen at runtime from data (tenant, route, message field) | `IBusProvider.GetBus(name)` |
| Diagnostics, admin endpoints, "show me every bus" surfaces | `IBusProvider.Buses` |

Prefer constructor injection (unkeyed or keyed) for anything static: it documents the
dependency and fails at container build rather than first use. Reach for the provider
only when the bus genuinely is not known until runtime.

## Bus modes

A bus declares its relationship with its broker up front via `BusMode`:

```csharp
public enum BusMode
{
    PublishAndConsume,  // the default
    PublishOnly,        // this app only publishes on this bus
    ConsumeOnly,        // this app only consumes on this bus
}
```

`Mode` is a settable property on the builder and may be assigned anywhere inside the
`AddBus` block:

```csharp
messaging.AddBus("partner-feed", bus =>
{
    bus.Mode = BusMode.ConsumeOnly;
    bus.AddTransport<KafkaConfig>(builder.Configuration.GetSection("Messaging:PartnerKafka"));
    bus.AddConsumer<PartnerPriceUpdateConsumer>();
});
```

Because the property may be set after other registrations, mode violations are validated
when the `AddBus` block completes — still configuration time, so the app fails at
startup. Modes are named by what they *permit* (there is deliberately no "read-only":
in messaging, consuming is reading, so the name would be ambiguous).

### Enforcement

Each violation fails as early as it possibly can:

| Violation | When it fails | Error |
| --- | --- | --- |
| `AddConsumer` on a `PublishOnly` bus | Configuration time, end of `AddBus` block | `Bus 'audit' is PublishOnly, but consumer 'OrderPlacedConsumer' was registered on it.` |
| Consume-side policy (`WithRetry`, `WithRetryForTopic`, `OnUnroutableMessage`) on a `PublishOnly` bus | Configuration time, end of block | `Bus 'audit' is PublishOnly, but the consume-side policy 'WithRetry' was configured on it.` |
| `AddOutbox` on a `ConsumeOnly` bus | Configuration time, end of block — the outbox exists to publish | (thrown by the outbox package's `OnValidate` callback) |
| `PublishAsync` / `PublishBatchAsync` on a `ConsumeOnly` bus | Runtime, at the call | `Bus 'partner-feed' is ConsumeOnly; publishing on it is not allowed.` |

### The dead-letter carve-out on `ConsumeOnly`

The framework's default dead-letter strategy (`DeadLetterStrategy.FrameworkManaged`)
*republishes* an exhausted or poison message to `{topic}.dead-letter` through the bus's
transport. The mode governs the **application** publishing surface (`IBus`) only;
framework-internal publishes performed by the consume pipeline — dead-letter enrichment —
remain allowed on a `ConsumeOnly` bus, because they are part of consuming.

If your broker credentials genuinely cannot write at all (read-only IAM policy, ACL),
change the strategy so the pipeline never publishes:

```csharp
messaging.AddBus("partner-feed", bus =>
{
    bus.Mode = BusMode.ConsumeOnly;
    bus.AddTransport<KafkaConfig>(builder.Configuration.GetSection("Messaging:PartnerKafka"));
    bus.AddConsumer<PartnerPriceUpdateConsumer>();
    bus.WithRetry(o => o.Strategy = DeadLetterStrategy.BrokerNative);  // broker-side DLQ
    // or: bus.WithRetry(o => o.EnableDeadLettering = false);          // log and drop
});
```

(`WithRetry` is a consume-side policy, so it is perfectly at home on a `ConsumeOnly`
bus — it is only forbidden on `PublishOnly`.)

### Resource effects

Beyond guardrails, the mode drives resource allocation:

- A `PublishOnly` bus registers **no** `MessageConsumerHostedService` — nothing consume-
  related runs for it.
- The mode is handed to the transport via `TransportContext.Mode`, so transports skip
  building the unused side entirely: a `ConsumeOnly` RabbitMQ bus creates no publish
  channel pool, a `ConsumeOnly` Kafka bus creates no producer, and a `PublishOnly` bus
  opens no subscription resources.

### Pair modes with least-privilege credentials

Modes turn credential assumptions into startup and call-site failures instead of opaque
broker errors deep in production. The pattern: give each bus credentials that match its
declared mode — a write-only SQS policy for a `PublishOnly` audit bus, a read-only
consumer account for a `ConsumeOnly` partner bus (with `BrokerNative` or disabled
dead-lettering, as above). If a code change later tries to consume on the audit bus, it
fails at startup with a named error, not at 3 a.m. with an `AccessDenied` from the broker.

## Multi-bus patterns

### Complete isolation — and shared defaults when you want them

Nothing configured on one bus leaks into another. Retry policies, serializers,
unroutable-message handlers, and outboxes are all keyed by bus name, and there is
deliberately **no global scope**. An application that wants shared defaults across buses
writes an `Action<BusBuilder>` helper and applies it to each — sharing by convention,
visible in the code, instead of sharing by hidden ambient state:

```csharp
Action<BusBuilder> standardPolicies = bus =>
{
    bus.WithRetry(o =>
    {
        o.MaxAttempts = 5;
        o.MaxDelay = TimeSpan.FromMinutes(1);
    });
    bus.OnUnroutableMessage(UnroutableStrategy.DeadLetter);
};

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "rabbit.orders.internal" });
        bus.AddConsumer<OrderPlacedConsumer>();
        standardPolicies(bus);
    });

    messaging.AddBus("billing", bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "rabbit.billing.internal" });
        bus.AddConsumer<InvoiceIssuedConsumer>();
        standardPolicies(bus);
        bus.WithRetryForTopic("invoices.issued", o => o.MaxAttempts = 10); // on top of the defaults
    });
});
```

### Same broker type on several buses

Any number of buses can use the same transport package — `SystemName` is a telemetry tag,
not an identity. The example above is exactly that: two RabbitMQ clusters, one bus each.
Each bus's config is validated and materialized independently — its own connection, its
own health check (`runax:default`, `runax:billing`), its own policies — and on a
dashboard the shared `messaging.system = "rabbitmq"` tag is disambiguated by
`messaging.runax.bus`. (In 1.x this was impossible: transports were registered by system
name, so a second cluster of the same broker had nowhere to live.)

### Same consumer type on several buses

A consumer class may be registered on more than one bus — `AddConsumer<T>()` on each.
Registrations are tracked per bus, so the consumer receives each bus's traffic through
**that bus's own pipeline**: the internal cluster's traffic with the default bus's retry
policy and serializer, the partner cluster's traffic with the partner bus's.

```csharp
messaging.AddBus(bus =>
{
    bus.AddTransport(new RabbitMqConfig { HostName = "rabbit.orders.internal" });
    bus.AddConsumer<PriceUpdateConsumer>();
    bus.WithRetry(o => o.MaxAttempts = 8);
});

messaging.AddBus("partner-feed", bus =>
{
    bus.Mode = BusMode.ConsumeOnly;
    bus.AddTransport<KafkaConfig>(builder.Configuration.GetSection("Messaging:PartnerKafka"));
    bus.AddConsumer<PriceUpdateConsumer>();          // same type, this bus's pipeline
    bus.WithRetry(o => o.MaxAttempts = 2);
});
```

One caveat: the consumer instance is a **shared singleton** (registered with
`TryAddSingleton`), so the same object handles both buses' messages. Keep consumers
stateless — take dependencies through the constructor, keep no per-message fields — and
this sharing is invisible.

### Cross-bus flows are explicit

There is no implicit routing, mirroring, or fallback between buses. Sending a message to
two brokers means injecting both buses and publishing on both:

```csharp
public sealed class OrderService(
    IBus bus,
    [FromKeyedServices("audit")] IBus audit)
{
    public async Task PlaceAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        await bus.PublishAsync("orders.placed", order, cancellationToken);   // fan-out:
        await audit.PublishAsync("audit.order-placed", order, cancellationToken);
    }
}
```

The two publishes are independent operations — there is no built-in atomic fan-out. If
the second publish must not be lost once the first succeeds, give each bus its own outbox
or add your own coordination. A consume-then-republish bridge is the same pattern from
inside a consumer: consume on one bus, inject the other bus, publish.

### Independent lifecycles

Each consuming bus runs its own `MessageConsumerHostedService`, so buses subscribe, run,
and shut down independently — a broker outage that tears down the `partner-feed`
subscription does not touch order processing on the default bus. Details below.

## Lifecycle

### Configuration time

`AddBus` validates the bus (see [above](#what-happens-when-the-block-completes)) and
registers keyed singletons for the transport and the bus, plus the transport's own
service registrations (health checks are registered here, eagerly). No broker connection
is made yet.

### Transports are created lazily

The transport is a keyed singleton whose factory calls
`TransportConfig.CreateTransport(new TransportContext(services, busName, mode))` — it
runs on the **first resolution** of the bus's transport and exactly once per bus.
In practice that is the first `PublishAsync` on the bus, or the moment the bus's consumer
hosted service first touches its transport during startup. A bus that is configured but
never used never connects to its broker.

### Per-bus consumer hosted service

`AddBus` registers one `MessageConsumerHostedService` for the bus **only if the block
registered at least one consumer** — a `PublishOnly` bus never gets one (consumers are
forbidden on it), and neither does a `PublishAndConsume` bus that happens to register no
consumers. Consuming therefore requires a .NET Generic Host; publishing does not.

On host start, each bus's service independently:

1. resolves the consumers registered on **its** bus and groups them by `Topic`
   (a bus with consumers but no effective topics logs and exits cleanly);
2. logs the subscription — `Bus 'partner-feed': subscribing to 2 topic(s) on 'kafka': ...`;
3. calls `IMessagingTransport.SubscribeAsync(topics, onMessage, stoppingToken)` and
   dispatches messages until shutdown, applying the bus's retry, dead-letter, and
   unroutable-message policies.

On host shutdown, each service's stopping token cancels its subscription — buses drain
and stop independently, in no particular mutual order.

### When one bus's subscription fails

Per-message failures never escape the dispatcher: a failing `HandleAsync` is retried and
dead-lettered per the bus's policy, and a malformed envelope is dead-lettered — the
subscription keeps running. A failure of the subscription itself (broker unreachable at
startup, connection torn down beyond the transport's own recovery) faults only that
bus's hosted service; the framework does not couple buses, so the other buses' services
keep consuming and publishing continues to work everywhere. What happens to the process
then is standard .NET hosting behavior: with the default
`HostOptions.BackgroundServiceExceptionBehavior.StopHost`, an unhandled fault in any
hosted service logs and stops the host — configure `Ignore` (and alert on the
`runax:{bus}` health check instead) if a degraded-but-running process is preferable for
your deployment.

## See also

- [Configuration & per-bus settings](configuration.md) — every `BusBuilder` setting and how bus/topic scoping resolves.
- [Publishing](publishing.md) — the publish pipeline, batching, headers, and the outbox sink.
- [Consuming](consuming.md) — writing consumers, retry and dead-lettering, contract versioning.
- [Observability](observability.md) — the bus-tagged spans, metrics, and `runax:{bus}` health checks.
- [Architecture & message flow](architecture.md) — package layering and the end-to-end flows.
- [Migrating from 1.x to 2.0](migrating-to-2.0.md) — mapping 1.x registration onto buses.
