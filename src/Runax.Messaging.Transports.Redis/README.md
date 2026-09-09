# Runax.Messaging.Transports.Redis

Redis Streams transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Works with **Redis** and **Valkey**. A topic maps to a stream key; consumption uses a consumer group.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Redis
```

## Register

Attach the transport to a bus with its config object:

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Redis;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RedisConfig
        {
            Configuration = "localhost:6379",
            ConsumerGroup = "orders-workers",
        });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`RedisConfig` lives in the `Runax.Messaging.Transports.Redis` namespace, so add that `using`.
The config is validated at startup (DataAnnotations, when the `AddBus` block completes); bind it
from an `IConfiguration` section instead of setting properties:
`bus.AddTransport<RedisConfig>(builder.Configuration.GetSection("Redis"))`.

`Configuration` is a [StackExchange.Redis connection string](https://stackexchange.github.io/StackExchange.Redis/Configuration).

## Config

Set these directly on `RedisConfig`.

| Property | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `Configuration` | StackExchange.Redis connection string. | (none) | Yes |
| `ConsumerGroup` | Consumer group used to read each stream. | `runax` | No |
| `ConsumerName` | This consumer's name within the group. | per-process unique | No |
| `ReadBatchSize` | Maximum entries read per poll. | `10` | No |
| `PollInterval` | Wait before polling again when a stream is idle. | `1s` | No |
| `ClaimIdleTime` | Idle time before a pending entry is reclaimed and redelivered. | `30s` | No |
| `RegisterHealthCheck` | Auto-register the `runax:{bus}` health check (inherited from `TransportConfig`). | `true` | No |

Beyond the transport config, settings from the core package apply to the bus that runs this
transport — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — all inside the same `AddBus` block.
See [Configuration & per-bus settings](../../docs/configuration.md).

## Behavior

- **Publish** appends the serialized envelope to the stream named after the topic (`XADD`).
- **Subscribe** ensures a consumer group per stream (created with the stream via `MKSTREAM`), then per
  stream reads new messages (`XREADGROUP >`) and reclaims idle-pending ones (`XAUTOCLAIM`), mapping the
  dispatch verdict:
  - `Acknowledge` → `XACK` (removed from the pending list)
  - `DeadLetter` → `XACK` (Redis has no native dead-letter; use the framework-managed DLQ to preserve)
  - `Requeue` → left pending; reclaimed after `ClaimIdleTime` and redelivered
- Idle-pending reclaim also recovers messages left behind by a crashed consumer.

## Health check

A reachability check named `runax:{bus}` is registered automatically for each bus that runs
this transport (`RegisterHealthCheck = false` on the config to opt out).

## Telemetry

The transport reports `messaging.system = "redis"` on the spans and metrics emitted by the core
package (activity source / meter `"Runax.Messaging"`); the `messaging.runax.bus` tag identifies
the bus.

## License

MIT
