# Runax.Messaging.RabbitMq

RabbitMQ transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Topics map to routing keys on a topic exchange.

> Built against `RabbitMQ.Client` 6.x.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.RabbitMq
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.RabbitMq;

builder.Services.AddRunaxMessaging(messaging => messaging
    .AddRabbitMq(options =>
    {
        options.HostName = "localhost";
    })
    .AddConsumer<OrderPlacedConsumer>());
```

`AddRabbitMq` lives in the `Runax.Messaging.RabbitMq` namespace, so add that `using`.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `HostName` | `localhost` | RabbitMQ host. |
| `Port` | `5672` | RabbitMQ port. |
| `UserName` | `guest` | Username. |
| `Password` | `guest` | Password. |
| `VirtualHost` | `/` | Virtual host. |
| `ExchangeName` | `runax.messaging` | Exchange to publish to and bind against. |
| `ExchangeType` | `topic` | Exchange type. |

## Behavior

- **Publish** sends the serialized envelope to `ExchangeName` using the topic as
  the routing key, with persistent delivery.
- **Subscribe** declares an exclusive, auto-delete queue, binds it to each topic,
  and acknowledges each message after your consumer handles it. A processing
  failure is logged and the message is nacked with requeue.

## License

MIT
