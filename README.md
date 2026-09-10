# Runax.Messaging

A lightweight publish/subscribe messaging library for .NET. A small core of
abstractions with a pluggable transport per broker — publish and consume
strongly-typed messages without coupling your application to a specific broker.

> **2.0: the bus model.** Configuration is structured around **buses** — a bus is a named,
> self-contained messaging context wrapping exactly one transport, plus the consumers,
> serialization, retry policy, and (optionally) outbox around it. Talking to several brokers
> means registering several buses, and `IBus` is the single publishing abstraction. Coming
> from 1.x? See [Migrating from 1.x to 2.0](docs/migrating-to-2.0.md).

- **Typed pub/sub** over a broker-agnostic `IBus` / `MessageConsumer<T>`.
- **Reliability** — retry with exponential backoff, poison-message handling, and
  framework-managed or broker-native dead-lettering.
- **Observability** — OpenTelemetry-ready tracing and metrics (no SDK dependency)
  plus an auto-registered health check per bus.
- **Throughput** — batch publish and concurrent SQS consumption.
- **Configurable** — DataAnnotations-validated transport configs with `IConfiguration`
  binding, and a pluggable body serializer set per bus or per topic (the `__runax`
  envelope stays framework-owned).
- **Contract versioning** — optional `[MessageContract(version)]`; consumers subscribe per version, with a
  pluggable strategy (dead-letter/requeue/custom) for versions no consumer handles.
- **Transactional outbox** — optional package for atomic database-write + publish.
- **Transports** — RabbitMQ, Apache Kafka, Amazon SQS, Amazon SNS, Azure Service Bus, Azure Event Hubs,
  Google Cloud Pub/Sub, Redis Streams (Redis/Valkey), and a built-in in-memory transport.

## Packages

| Package | Description |
| --- | --- |
| [`Runax.Messaging.Abstractions`](src/Runax.Messaging.Abstractions/README.md) | Contracts only: `IBus`, `IBusProvider`, the `IMessagingTransport` / `TransportConfig` SPI, and `MessageContext`. Reference this from application and transport code. |
| [`Runax.Messaging`](src/Runax.Messaging/README.md) | Default implementation: DI wiring (`AddBus` / `BusBuilder`), JSON serialization, hosted consumers, and an in-memory transport. |
| [`Runax.Messaging.Transports.Aws.Sqs`](src/Runax.Messaging.Transports.Aws.Sqs/README.md) | Amazon SQS transport. |
| [`Runax.Messaging.Transports.Aws.Sns`](src/Runax.Messaging.Transports.Aws.Sns/README.md) | Amazon SNS transport (publish to SNS, consume via SQS). |
| [`Runax.Messaging.Transports.Azure.ServiceBus`](src/Runax.Messaging.Transports.Azure.ServiceBus/README.md) | Azure Service Bus transport. |
| [`Runax.Messaging.Transports.Azure.EventHubs`](src/Runax.Messaging.Transports.Azure.EventHubs/README.md) | Azure Event Hubs transport. |
| [`Runax.Messaging.Transports.RabbitMq`](src/Runax.Messaging.Transports.RabbitMq/README.md) | RabbitMQ transport. |
| [`Runax.Messaging.Transports.Kafka`](src/Runax.Messaging.Transports.Kafka/README.md) | Apache Kafka transport. |
| [`Runax.Messaging.Transports.Google.PubSub`](src/Runax.Messaging.Transports.Google.PubSub/README.md) | Google Cloud Pub/Sub transport. |
| [`Runax.Messaging.Transports.Redis`](src/Runax.Messaging.Transports.Redis/README.md) | Redis Streams transport (Redis and Valkey). |
| [`Runax.Messaging.Outbox`](src/Runax.Messaging.Outbox/README.md) | Transactional outbox: persist in your DB transaction, dispatch reliably. |
| [`Runax.Messaging.TestKit`](src/Runax.Messaging.TestKit/README.md) | Test-support: a broker-free `MessagingTestHarness` to publish messages and assert what your consumers handled, retried, or dead-lettered. |

Application code that only publishes needs `Runax.Messaging.Abstractions`. The
composition root (where you call `AddRunaxMessaging`) needs `Runax.Messaging`
plus one transport package.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Aws.Sqs        # or .Transports.RabbitMq, or use the built-in in-memory transport
```

## Quick start

Register messaging, add a bus with a transport, and add consumers:

```csharp
using Runax.Messaging;              // AddRunaxMessaging, AddBus, MessageConsumer<T>
using Runax.Messaging.Abstractions; // IBus
using Runax.Messaging.InMemory;     // InMemoryConfig

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new InMemoryConfig());   // transport: in-process (great for tests / single process)
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});

var host = builder.Build();
```

Publish a message by injecting `IBus` (the unkeyed injection resolves the default bus):

```csharp
public sealed class Checkout(IBus bus)
{
    public ValueTask PlaceOrderAsync(Order order) =>
        bus.PublishAsync("orders.placed", order);
}
```

Consume by deriving from `MessageConsumer<TMessage>`:

```csharp
using Runax.Messaging;

public sealed class OrderPlacedConsumer : MessageConsumer<Order>
{
    public override string Topic => "orders.placed";

    protected override ValueTask HandleAsync(Order order, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Received order {order.Id}");
        return ValueTask.CompletedTask;
    }
}
```

Consumers are dispatched by a hosted background service, so consuming requires a
.NET Generic Host (`Microsoft.Extensions.Hosting`). Publishing does not.

## Switching transports

Only the composition root changes; publishers and consumers stay the same:

```csharp
// Amazon SQS
using Runax.Messaging.Transports.Aws.Sqs;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});

// RabbitMQ
using Runax.Messaging.Transports.RabbitMq;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

Every transport is attached the same way: `bus.AddTransport(...)` takes the transport's
config object (a `<Broker>Config` deriving `TransportConfig`), and `bus.AddConsumer<T>()`
binds a consumer to that bus. Configs can also be built with a delegate
(`bus.AddTransport<RabbitMqConfig>(c => c.HostName = "localhost")`) or bound from
configuration (`bus.AddTransport<RabbitMqConfig>(builder.Configuration.GetSection("RabbitMq"))`).

Each transport's config properties are documented on its package page linked in the table
above.

## Multiple buses

A bus wraps **exactly one** transport (a second `AddTransport` on the same bus throws at
configuration time), so talking to more than one broker — even two clusters of the same
broker — means registering more than one bus:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>                          // the default bus: unkeyed IBus resolves it
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
    });

    messaging.AddBus("audit", bus =>                 // a named bus on a different broker
    {
        bus.Mode = BusMode.PublishOnly;              // optional: publishes only; AddConsumer here throws
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
    });
});
```

Each bus is fully isolated — its own transport connection, consumers, retry policy,
serializers, health check, and hosted lifecycle. Register the same consumer type on two
buses to consume its topic from both brokers (it stays a single instance; each bus's
traffic flows through that bus's own pipeline). Named buses resolve via keyed DI or
`IBusProvider`:

```csharp
public sealed class AuditTrail([FromKeyedServices("audit")] IBus audit)
{
    public ValueTask RecordAsync(AuditEntry entry, CancellationToken ct) =>
        audit.PublishAsync("audit.entry", entry, ct);
}
```

To publish the same event to **several** brokers, inject each bus and publish on both:

```csharp
public sealed class OrderService(
    IBus main,
    [FromKeyedServices("events")] IBus events)
{
    public async Task PlaceAsync(OrderPlaced order, CancellationToken ct)
    {
        await main.PublishAsync("orders", order, ct);
        await events.PublishAsync("orders", order, ct);
    }
}
```

The sends are independent (no atomic fan-out). `IBusProvider.GetBus("name")` /
`IBusProvider.Buses` cover dynamic lookup and diagnostics.

## Reliability & observability

Consumers get retry-with-backoff, poison-message handling, and dead-lettering out
of the box; tune them with `WithRetry(...)` — per bus, or per topic on a bus (the
most specific scope wins):

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 5);                        // this bus's policy
        bus.WithRetryForTopic("payments", o => o.MaxAttempts = 10);   // override for one topic
    });
});
```

`WithRetry`, `OnUnroutableMessage`, `ConfigureSerialization`, and `UseSerializer<T>()` are
all bus-scoped, with `*ForTopic` variants for topic-level overrides. See
[Configuration & per-bus settings](docs/configuration.md) for the full table and fallback rules.

Publish/consume are traced and metered via the in-box `System.Diagnostics` APIs —
subscribe an OpenTelemetry pipeline with `AddSource("Runax.Messaging")` and
`AddMeter("Runax.Messaging")`; every span and metric carries a `messaging.runax.bus` tag.
Each bus auto-registers a broker health check named `runax:{bus}` (opt out with
`RegisterHealthCheck = false` on the transport config). See
[Architecture & message flow](docs/architecture.md) for details.

## Throughput & the outbox

Publish many messages at once with `bus.PublishBatchAsync(topic, messages)` (SQS
`SendMessageBatch`; a single RabbitMQ confirm per batch), and tune SQS concurrency with
`MaxConcurrentMessages`. For atomic "save + publish", add the
[`Runax.Messaging.Outbox`](src/Runax.Messaging.Outbox/README.md) package and configure it
on the bus (`bus.AddOutbox()` + `bus.AddOutboxStore(...)`) so publishes are written to
your database in the same transaction and dispatched by a background service.

## Documentation

Start at the [documentation index](docs/README.md).

**Concepts & guides**

- [Buses — the core model](docs/buses.md): one transport per bus, modes, multi-bus patterns, resolution rules
- [Publishing](docs/publishing.md): `IBus`, headers, batching, fan-out, what a publish actually does
- [Consuming & reliability](docs/consuming.md): consumers, contract versioning, retries, dead-lettering, unroutable messages
- [Transactional outbox](docs/outbox.md): the pattern, wiring, writing a real store, delivery guarantees
- [Testing](docs/testing.md): the TestKit harness, multi-bus tests, integration tests
- [Observability](docs/observability.md): tracing, metrics, health checks, logging

**Reference**

- [Migrating from 1.x to 2.0](docs/migrating-to-2.0.md)
- [Configuration & per-bus settings](docs/configuration.md)
- [Architecture & message flow](docs/architecture.md)
- [Serialization & custom serializers](docs/serialization.md)
- [Writing a custom transport](docs/writing-a-custom-transport.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE).
