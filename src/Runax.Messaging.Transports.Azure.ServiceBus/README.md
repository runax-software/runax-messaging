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

builder.Services.AddRunaxMessaging(messaging => messaging
    .AddAzureServiceBus(sb => sb.Configure(options =>
    {
        options.ConnectionString = "<your Service Bus connection string>";
        options.TopicSubscriptionMap["orders.placed"] = "orders-worker"; // topic -> subscription
    }))
    .AddConsumer<OrderPlacedConsumer>());
```

`AddAzureServiceBus` lives in the `Runax.Messaging.Transports.Azure.ServiceBus` namespace, so add that
`using`. Options are validated at startup; pass an `IConfiguration` section to bind them instead of a
lambda: `AddAzureServiceBus(builder.Configuration.GetSection("ServiceBus"))`.

Provision the topics and subscriptions ahead of time.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `ConnectionString` | (required) | Service Bus connection string. |
| `TopicSubscriptionMap` | empty | Topic → subscription used to consume it. Required to consume a topic. |
| `MaxConcurrentCalls` | `1` | Messages processed concurrently per subscription. |

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
