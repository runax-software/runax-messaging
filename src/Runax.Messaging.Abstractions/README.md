# Runax.Messaging.Abstractions

Core publish/subscribe contracts for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
This package has almost no dependencies — reference it from application code that
only needs to publish, and from transport packages that implement the SPI.

## Install

```bash
dotnet add package Runax.Messaging.Abstractions
```

## What's inside

| Type | Role |
| --- | --- |
| `IMessagePublisher` | Publishes a strongly-typed message (or a batch via `PublishBatchAsync`) to a topic, optionally with headers. |
| `IMessagePublisherFactory` | Resolves an `IMessagePublisher` pinned to a named transport (`ForTransport("<system-name>")`) so you can publish to several transports explicitly. |
| `IMessagingTransport` | Provider SPI. Each transport implements broker-specific publish (single and batch) / subscribe and exposes a `SystemName` telemetry tag. |
| `MessageContext` | A received message: topic, raw JSON body, headers, and a `Deserialize<T>()` helper. |
| `MessageDisposition` | The verdict a transport applies after dispatch: `Acknowledge`, `Requeue`, or `DeadLetter`. |
| `PoisonMessageException` | Thrown by a consumer to skip retries and dead-letter the message immediately. |
| `MessagingConfigurator` | Fluent builder that transports and consumers attach to via extension methods. |

## Usage

Depend on `IMessagePublisher` wherever you publish:

```csharp
using Runax.Messaging.Abstractions;

public sealed class Checkout(IMessagePublisher publisher)
{
    public ValueTask PlaceOrderAsync(Order order) =>
        publisher.PublishAsync("orders.placed", order);
}
```

The implementation, transports, and hosting live in
[`Runax.Messaging`](https://www.nuget.org/packages/Runax.Messaging) and the
transport packages.

## License

MIT
