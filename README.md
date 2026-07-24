# Runax.Messaging

A lightweight publish/subscribe messaging library for .NET. A small core of
abstractions with a pluggable transport per broker — publish and consume
strongly-typed messages without coupling your application to a specific broker.

- **Typed pub/sub** over a broker-agnostic `IMessagePublisher` / `MessageConsumer<T>`.
- **Reliability** — retry with exponential backoff, poison-message handling, and
  framework-managed or broker-native dead-lettering.
- **Observability** — OpenTelemetry-ready tracing and metrics (no SDK dependency)
  plus per-transport health checks.
- **Throughput** — batch publish and concurrent SQS consumption.
- **Configurable** — validated options with `IConfiguration` binding, and a pluggable body
  serializer set globally or per broker (the `__runax` envelope stays framework-owned).
- **Contract versioning** — optional `[MessageContract(version)]`; consumers subscribe per version, with a
  pluggable strategy (dead-letter/requeue/custom) for versions no consumer handles.
- **Transactional outbox** — optional package for atomic database-write + publish.
- **Transports** — RabbitMQ, Amazon SQS, Amazon SNS, Azure Service Bus, Google Cloud Pub/Sub,
  Redis Streams (Redis/Valkey), and a built-in in-memory transport.

## Packages

| Package | Description |
| --- | --- |
| [`Runax.Messaging.Abstractions`](src/Runax.Messaging.Abstractions/README.md) | Contracts only: `IMessagePublisher`, the `IMessagingTransport` SPI, `MessageContext`, and the `MessagingConfigurator` builder. Reference this from application and transport code. |
| [`Runax.Messaging`](src/Runax.Messaging/README.md) | Default implementation: DI wiring, JSON serialization, hosted consumers, and an in-memory transport. |
| [`Runax.Messaging.Transports.Aws.Sqs`](src/Runax.Messaging.Transports.Aws.Sqs/README.md) | Amazon SQS transport. |
| [`Runax.Messaging.Transports.Aws.Sns`](src/Runax.Messaging.Transports.Aws.Sns/README.md) | Amazon SNS transport (publish to SNS, consume via SQS). |
| [`Runax.Messaging.Transports.Azure.ServiceBus`](src/Runax.Messaging.Transports.Azure.ServiceBus/README.md) | Azure Service Bus transport. |
| [`Runax.Messaging.Transports.RabbitMq`](src/Runax.Messaging.Transports.RabbitMq/README.md) | RabbitMQ transport. |
| [`Runax.Messaging.Transports.Google.PubSub`](src/Runax.Messaging.Transports.Google.PubSub/README.md) | Google Cloud Pub/Sub transport. |
| [`Runax.Messaging.Transports.Redis`](src/Runax.Messaging.Transports.Redis/README.md) | Redis Streams transport (Redis and Valkey). |
| [`Runax.Messaging.Outbox`](src/Runax.Messaging.Outbox/README.md) | Transactional outbox: persist in your DB transaction, dispatch reliably. |

Application code that only publishes needs `Runax.Messaging.Abstractions`. The
composition root (where you call `AddRunaxMessaging`) needs `Runax.Messaging`
plus one transport package.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Aws.Sqs        # or .Transports.RabbitMq, or use the built-in in-memory transport
```

## Quick start

Register messaging, pick a transport, and add consumers:

```csharp
using Runax.Messaging;              // AddRunaxMessaging, AddInMemory, AddConsumer, MessageConsumer<T>
using Runax.Messaging.Abstractions; // IMessagePublisher

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddInMemory(inMemory =>       // transport: in-process (great for tests / single process)
    {
        inMemory.AddConsumer<OrderPlacedConsumer>();
    });
});

var host = builder.Build();
```

Publish a message by injecting `IMessagePublisher`:

```csharp
public sealed class Checkout(IMessagePublisher publisher)
{
    public ValueTask PlaceOrderAsync(Order order) =>
        publisher.PublishAsync("orders.placed", order);
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

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddSqs(sqs =>
    {
        sqs.Configure(o => o.Region = "us-east-1");
        sqs.AddConsumer<OrderPlacedConsumer>();
    });
});

// RabbitMQ
using Runax.Messaging.Transports.RabbitMq;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
    });
});
```

Each transport is registered with a single block: `Configure(o => ...)` sets its options, and
`AddConsumer<T>()` (inside the block) binds a consumer to that broker.

Each transport's options are documented on its package page linked in the table
above.

## Multiple transports at once

Register more than one transport and a single consumer can receive its topic from several brokers —
even different ones (e.g. RabbitMQ and SQS during a migration). Transports are identified by their
`SystemName` (`"rabbitmq"`, `"sqs"`, `"in-memory"`, ...):

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<AuditConsumer>();          // scoped: only RabbitMQ
    });

    runax.AddSqs(sqs =>
    {
        sqs.Configure(o => o.Region = "us-east-1");
    });

    runax.AddConsumer<OrderPlacedConsumer>();           // top-level: every registered transport
    runax.PublishTo("sqs");                             // IMessagePublisher publishes here
});
```

A consumer registered **inside a transport's block** subscribes only on that broker; a **top-level**
`AddConsumer<T>()` subscribes on every registered transport. Register the same consumer under two brokers to
consume from both — it stays a single instance. Each transport is subscribed and dispatched independently, so
a message is only handled by consumers bound to the broker it arrived on. When several transports are
registered, `PublishTo("<system-name>")` selects which one `IMessagePublisher` publishes to (a single
registered transport is used automatically). Each transport must report a distinct `SystemName`.

## Reliability & observability

Consumers get retry-with-backoff, poison-message handling, and dead-lettering out
of the box; tune them with `WithRetry(...)`:

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
    });

    runax.WithRetry(o => o.MaxAttempts = 5);
});
```

Publish/consume are traced and metered via the in-box `System.Diagnostics` APIs —
subscribe an OpenTelemetry pipeline with `AddSource("Runax.Messaging")` and
`AddMeter("Runax.Messaging")`, and add broker health checks with
`AddRabbitMqTransport()` / `AddSqsTransport()`. See
[Architecture & message flow](docs/architecture.md) for details.

## Throughput & the outbox

Publish many messages at once with `publisher.PublishBatchAsync(topic, messages)` (SQS
`SendMessageBatch`; a single RabbitMQ confirm per batch), and tune SQS concurrency with
`MaxConcurrentMessages`. For atomic "save + publish", add the
[`Runax.Messaging.Outbox`](src/Runax.Messaging.Outbox/README.md) package so publishes are
written to your database in the same transaction and dispatched by a background service.

## Documentation

- [Architecture & message flow](docs/architecture.md)
- [Serialization & custom serializers](docs/serialization.md)
- [Writing a custom transport](docs/writing-a-custom-transport.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE).
