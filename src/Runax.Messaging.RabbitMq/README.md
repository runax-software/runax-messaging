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
| `PrefetchCount` | `10` | Unacknowledged messages allowed in flight per consumer (`BasicQos`). |
| `PublisherConfirms` | `true` | Wait for broker acknowledgement of each publish. |
| `ConfirmTimeout` | `5s` | How long to wait for a publisher confirm before failing. |
| `PublishChannelPoolSize` | `5` | Size of the publish channel pool; larger allows more concurrent publishes. |
| `DeadLetterExchange` | `null` | Dead-letter exchange for broker-native dead-lettering. When set, consumer queues are declared with `x-dead-letter-exchange`. |
| `DeadLetterExchangeType` | `topic` | Type of the declared `DeadLetterExchange`. |

## Behavior

- **Publish** sends the serialized envelope to `ExchangeName` using the topic as
  the routing key, with persistent delivery. Publishes fan out across a pool of
  channels (`PublishChannelPoolSize`) and, when `PublisherConfirms` is on, wait for
  broker confirmation. Automatic and topology recovery are enabled.
- **Subscribe** declares an exclusive, auto-delete queue (with `BasicQos` prefetch),
  binds it to each topic, and maps the dispatch verdict to the broker:
  - `Acknowledge` → `basic.ack`
  - `Requeue` → `basic.nack` with requeue
  - `DeadLetter` → `basic.nack` without requeue, routed to `DeadLetterExchange` when
    configured (otherwise dropped by the broker)

To use RabbitMQ's native dead-lettering, set `DeadLetterExchange` and configure
`WithRetry(o => o.Strategy = DeadLetterStrategy.BrokerNative)` in the core registration.

## Health check

Register a broker-reachability check on `IHealthChecksBuilder`:

```csharp
builder.Services.AddHealthChecks().AddRabbitMqTransport();
```

## Telemetry

The transport reports `messaging.system = "rabbitmq"` on the spans and metrics
emitted by the core package (activity source / meter `"Runax.Messaging"`).

## License

MIT
