# Runax.Messaging.Transports.Redis

Redis Streams transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Works with **Redis** and **Valkey**. A topic maps to a stream key; consumption uses a consumer group.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Redis
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Redis;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRedis(redis =>
    {
        redis.Configure(options =>
        {
            options.Configuration = "localhost:6379";
            options.ConsumerGroup = "orders-workers";
        });
        redis.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddRedis` lives in the `Runax.Messaging.Transports.Redis` namespace, so add that `using`.
Options are validated at startup; pass an `IConfiguration` section to bind them instead of a lambda:
`AddRedis(builder.Configuration.GetSection("Redis"))`.

`Configuration` is a [StackExchange.Redis connection string](https://stackexchange.github.io/StackExchange.Redis/Configuration).

## Options

Configure these on `RedisOptions` via `redis.Configure(o => ...)`.

| Option | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `Configuration` | StackExchange.Redis connection string. | (none) | Yes |
| `ConsumerGroup` | Consumer group used to read each stream. | `runax` | No |
| `ConsumerName` | This consumer's name within the group. | per-process unique | No |
| `ReadBatchSize` | Maximum entries read per poll. | `10` | No |
| `PollInterval` | Wait before polling again when a stream is idle. | `1s` | No |
| `ClaimIdleTime` | Idle time before a pending entry is reclaimed and redelivered. | `30s` | No |

Beyond these transport options, settings from the core package can be applied to this broker
inside the `AddRedis(...)` block — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — each overriding the global default for
Redis only. See [Configuration & per-broker settings](../../docs/configuration.md).

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

Register a reachability check on `IHealthChecksBuilder`:

```csharp
builder.Services.AddHealthChecks().AddRedisTransport();
```

## Telemetry

The transport reports `messaging.system = "redis"` on the spans and metrics emitted by the core
package (activity source / meter `"Runax.Messaging"`).

## License

MIT
