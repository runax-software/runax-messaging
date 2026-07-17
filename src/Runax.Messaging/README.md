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
invokes your configuration. Register exactly one transport. If any consumers are
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

## In-memory transport

`AddInMemory()` delivers messages in-process through channels, one per topic. It
is intended for tests and single-process scenarios — messages are not persisted
and do not cross process boundaries.

## License

MIT
