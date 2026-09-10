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
| `IBus` | The single application handle to messaging: publishes a strongly-typed message (or a batch via `PublishBatchAsync`) to a topic on the bus's transport, optionally with headers. Inject unkeyed for the default bus, keyed by name for a named bus. |
| `IBusProvider` | Resolves buses dynamically (`GetBus("<name>")`) and enumerates them (`Buses`) for diagnostics and admin surfaces. |
| `BusMode` / `BusNames` | A bus's declared mode (`PublishAndConsume` / `PublishOnly` / `ConsumeOnly`) and the well-known default bus name. |
| `TransportConfig` / `TransportContext` | Transport SPI: a transport package derives one config type carrying its broker settings and the `CreateTransport` factory; the context hands it the bus name, mode, and service provider. |
| `IMessagingTransport` | Provider SPI. Each transport implements broker-specific publish (single and batch) / subscribe and exposes a `SystemName` telemetry tag. |
| `MessageContext` | A received message: topic, raw JSON body, headers, and a `Deserialize<T>()` helper. |
| `MessageDisposition` | The verdict a transport applies after dispatch: `Acknowledge`, `Requeue`, or `DeadLetter`. |
| `PoisonMessageException` | Thrown by a consumer to skip retries and dead-letter the message immediately. |
| `MessagingConfigurator` | The `AddRunaxMessaging` surface; buses attach via the `AddBus` extensions in the `Runax.Messaging` package. |

## Usage

Depend on `IBus` wherever you publish:

```csharp
using Runax.Messaging.Abstractions;

public sealed class Checkout(IBus bus)
{
    public ValueTask PlaceOrderAsync(Order order) =>
        bus.PublishAsync("orders.placed", order);
}
```

An unkeyed `IBus` resolves the application's default bus; a named bus resolves via keyed DI
(`[FromKeyedServices("audit")] IBus`) or `IBusProvider.GetBus("audit")`.

The implementation, transports, and hosting live in
[`Runax.Messaging`](https://www.nuget.org/packages/Runax.Messaging) and the
transport packages.

## License

MIT
