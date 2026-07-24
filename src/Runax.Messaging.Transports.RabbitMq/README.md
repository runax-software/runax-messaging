# Runax.Messaging.Transports.RabbitMq

RabbitMQ transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Topics map to routing keys on a topic exchange.

> Built against `RabbitMQ.Client` 7.x (the asynchronous `IChannel` API).

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.RabbitMq
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.RabbitMq;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(options =>
        {
            options.HostName = "localhost";
        });
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddRabbitMq` lives in the `Runax.Messaging.Transports.RabbitMq` namespace, so add that `using`.
Options are validated at startup; pass an `IConfiguration` section to bind them instead of a
lambda: `AddRabbitMq(builder.Configuration.GetSection("RabbitMq"))`.

## Options

Configure these on `RabbitMqOptions` via `rabbitmq.Configure(o => ...)`.

| Option | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `HostName` | RabbitMQ host. Ignored when `Uri` is set. | `localhost` | No |
| `Port` | RabbitMQ port. Ignored when `Uri` is set. | `5672` | No |
| `UserName` | Username. Ignored when `Uri` is set. | `guest` | No |
| `Password` | Password. Prefer `Uri` (an `amqps://` string) or a secret store. Ignored when `Uri` is set. | `guest` | No |
| `VirtualHost` | Virtual host. Ignored when `Uri` is set. | `/` | No |
| `Uri` | Full AMQP connection URI (e.g. `amqps://user:pass@host:5671/vhost`). Takes precedence over the discrete settings and enables TLS for the `amqps` scheme. | `null` | No |
| `UseTls` | Enable TLS when connecting via the discrete host settings (not `Uri`). | `false` | No |
| `SslServerName` | TLS server name (SNI) when `UseTls` is on. Defaults to `HostName`. | `null` | No |
| `ExchangeName` | Exchange to publish to and bind against. | `runax.messaging` | No |
| `ExchangeType` | Exchange type. | `topic` | No |
| `PrefetchCount` | Unacknowledged messages allowed in flight per consumer (`BasicQos`). | `10` | No |
| `PublisherConfirms` | Wait for broker acknowledgement of each publish. | `true` | No |
| `ConfirmTimeout` | How long to wait for a publisher confirm before failing. | `5s` | No |
| `PublishChannelPoolSize` | Size of the publish channel pool; larger allows more concurrent publishes. | `5` | No |
| `DeadLetterExchange` | Dead-letter exchange for broker-native dead-lettering. When set, consumer queues are declared with `x-dead-letter-exchange`. | `null` | No |
| `DeadLetterExchangeType` | Type of the declared `DeadLetterExchange`. | `topic` | No |

Beyond these transport options, settings from the core package can be applied to this broker
inside the `AddRabbitMq(...)` block — `AddConsumer<T>()`, `WithRetry(...)`,
`OnUnroutableMessage(...)`, `ConfigureSerialization(...)`, and `UseSerializer<T>()` — each
overriding the global default for RabbitMQ only. See
[Configuration & per-broker settings](../../docs/configuration.md).

## Behavior

- **Publish** sends the serialized envelope to `ExchangeName` using the topic as
  the routing key, with persistent delivery. Publishes fan out across a pool of
  channels (`PublishChannelPoolSize`) and, when `PublisherConfirms` is on, wait for
  broker confirmation. Automatic and topology recovery are enabled.
- **Batch publish** (`PublishBatchAsync`) publishes all envelopes on one channel and
  waits for a single confirm for the whole batch instead of one per message.
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
