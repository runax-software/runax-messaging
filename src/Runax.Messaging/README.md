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
