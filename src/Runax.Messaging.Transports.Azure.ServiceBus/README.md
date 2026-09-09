# Runax.Messaging.Transports.Azure.ServiceBus

Azure Service Bus transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
A topic maps to a Service Bus **topic** for publishing and to a **subscription** for consuming.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Azure.ServiceBus
```

## Register

Attach the transport to a bus with its config object:

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Azure.ServiceBus;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new AzureServiceBusConfig
        {
            ConnectionString = "<your Service Bus connection string>",
            TopicSubscriptionMap =
            {
                ["orders.placed"] = "orders-worker",   // topic -> subscription
            },
        });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AzureServiceBusConfig` lives in the `Runax.Messaging.Transports.Azure.ServiceBus` namespace, so add
that `using`. The config is validated at startup (DataAnnotations, when the `AddBus` block
completes); bind it from an `IConfiguration` section instead of setting properties:
`bus.AddTransport<AzureServiceBusConfig>(builder.Configuration.GetSection("ServiceBus"))`.

Provision the topics and subscriptions ahead of time.

## Config

Set these directly on `AzureServiceBusConfig`.

| Property | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `ConnectionString` | Service Bus connection string. | (none) | Yes |
| `TopicSubscriptionMap` | Topic → subscription used to consume it. | empty | To consume a topic |
| `MaxConcurrentCalls` | Messages processed concurrently per subscription. | `1` | No |
| `RegisterHealthCheck` | Auto-register the `runax:{bus}` health check (inherited from `TransportConfig`). | `true` | No |

Beyond the transport config, settings from the core package apply to the bus that runs this
transport — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — all inside the same `AddBus` block.
See [Configuration & per-bus settings](../../docs/configuration.md).

## Behavior

- **Publish** sends the serialized envelope to the Service Bus topic named after the runax topic.
- **Subscribe** runs a `ServiceBusProcessor` per topic over its mapped subscription and maps the
  dispatch verdict to the message:
  - `Acknowledge` → complete
  - `Requeue` → abandon (returned for redelivery)
  - `DeadLetter` → dead-letter (Service Bus's **native** dead-letter queue for the subscription)

## Health check

A check named `runax:{bus}` is registered automatically for each bus that runs this transport
(`RegisterHealthCheck = false` on the config to opt out). The check fetches namespace properties
through the management endpoint, so it requires management access to the namespace.

## Telemetry

The transport reports `messaging.system = "servicebus"` on the spans and metrics emitted by the core
package (activity source / meter `"Runax.Messaging"`); the `messaging.runax.bus` tag identifies
the bus.

## License

MIT
