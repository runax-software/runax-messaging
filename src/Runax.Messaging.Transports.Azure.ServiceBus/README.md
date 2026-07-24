# Runax.Messaging.Transports.Azure.ServiceBus

Azure Service Bus transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
A topic maps to a Service Bus **topic** for publishing and to a **subscription** for consuming.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Azure.ServiceBus
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Azure.ServiceBus;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddAzureServiceBus(serviceBus =>
    {
        serviceBus.Configure(options =>
        {
            options.ConnectionString = "<your Service Bus connection string>";
            options.TopicSubscriptionMap["orders.placed"] = "orders-worker"; // topic -> subscription
        });
        serviceBus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddAzureServiceBus` lives in the `Runax.Messaging.Transports.Azure.ServiceBus` namespace, so add that
`using`. Options are validated at startup; pass an `IConfiguration` section to bind them instead of a
lambda: `AddAzureServiceBus(builder.Configuration.GetSection("ServiceBus"))`.

Provision the topics and subscriptions ahead of time.

## Options

Configure these on `AzureServiceBusOptions` via `serviceBus.Configure(o => ...)`.

| Option | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `ConnectionString` | Service Bus connection string. | (none) | Yes |
| `TopicSubscriptionMap` | Topic → subscription used to consume it. | empty | To consume a topic |
| `MaxConcurrentCalls` | Messages processed concurrently per subscription. | `1` | No |

Beyond these transport options, settings from the core package can be applied to this broker
inside the `AddAzureServiceBus(...)` block — `AddConsumer<T>()`, `WithRetry(...)`,
`OnUnroutableMessage(...)`, `ConfigureSerialization(...)`, and `UseSerializer<T>()` — each
overriding the global default for Service Bus only. See
[Configuration & per-broker settings](../../docs/configuration.md).

## Behavior

- **Publish** sends the serialized envelope to the Service Bus topic named after the runax topic.
- **Subscribe** runs a `ServiceBusProcessor` per topic over its mapped subscription and maps the
  dispatch verdict to the message:
  - `Acknowledge` → complete
  - `Requeue` → abandon (returned for redelivery)
  - `DeadLetter` → dead-letter (Service Bus's **native** dead-letter queue for the subscription)

## Health check

```csharp
builder.Services.AddHealthChecks().AddAzureServiceBusTransport();
```

The check fetches namespace properties through the management endpoint, so it requires management
access to the namespace.

## Telemetry

The transport reports `messaging.system = "servicebus"` on the spans and metrics emitted by the core
package (activity source / meter `"Runax.Messaging"`).

## License

MIT
