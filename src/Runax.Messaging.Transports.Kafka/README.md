# Runax.Messaging.Transports.Kafka

Apache Kafka transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
A topic maps directly to a Kafka topic; publishing produces the envelope as the record value and
consuming uses a consumer group with manual offset commits.

> Built against `Confluent.Kafka` 2.x.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Kafka
```

## Register

Attach the transport to a bus with its config object:

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Kafka;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new KafkaConfig { BootstrapServers = "localhost:9092" });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`KafkaConfig` lives in the `Runax.Messaging.Transports.Kafka` namespace, so add that `using`.
The config is validated at startup (DataAnnotations, when the `AddBus` block completes); bind it
from an `IConfiguration` section instead of setting properties:
`bus.AddTransport<KafkaConfig>(builder.Configuration.GetSection("Kafka"))`.

## Config

Set these directly on `KafkaConfig`.

| Property | Default | Description |
| --- | --- | --- |
| `BootstrapServers` | *(required)* | Comma-separated `host:port` bootstrap servers (e.g. `localhost:9092`). |
| `ConsumerGroupId` | `runax` | Consumer group id used when subscribing; offsets are committed per group. |
| `AutoOffsetReset` | `earliest` | Where a new group starts when it has no committed offset: `earliest` or `latest`. |
| `SecurityProtocol` | `null` | SASL/SSL security protocol (e.g. `SaslSsl`, `Ssl`). Plaintext when unset. |
| `SaslMechanism` | `null` | SASL mechanism (e.g. `Plain`, `ScramSha256`). |
| `SaslUsername` | `null` | SASL username. |
| `SaslPassword` | `null` | SASL password. Prefer a secret store over a plaintext value. |
| `Acks` | `all` | Producer acknowledgement level: `all`, `leader`, or `none`. |
| `EnableIdempotence` | `true` | Idempotent production so retries do not create duplicates. |
| `DeadLetterTopicSuffix` | `.dead-letter` | Suffix appended to a topic to form its dead-letter topic. |
| `PollTimeout` | `1s` | How long a single consumer poll blocks before looping. |
| `RegisterHealthCheck` | `true` | Auto-register the `runax:{bus}` health check (inherited from `TransportConfig`). |

Beyond the transport config, settings from the core package apply to the bus that runs this
transport — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — all inside the same `AddBus` block.
Kafka for events and Kafka for audit are simply two buses, each with its own `KafkaConfig`. See
[Configuration & per-bus settings](../../docs/configuration.md).

## Behavior

- **Publish** produces the serialized envelope as the value of a record on the topic
  (`ProduceAsync`), awaiting the broker delivery report. Idempotent production is on
  by default. A `ConsumeOnly` bus creates no producer.
- **Batch publish** (`PublishBatchAsync`) pipelines every produce and awaits all
  delivery reports for the batch together.
- **Subscribe** joins `ConsumerGroupId`, subscribes to each topic, and polls on a
  dedicated background thread with manual offset commits. Kafka has no per-message
  ack or native dead-letter queue, so the dispatch verdict is mapped onto offset
  control:
  - `Acknowledge` → **commit** the record's offset so it is not redelivered.
  - `Requeue` → **don't commit**, and `seek` back to the record so the next poll
    redelivers it.
  - `DeadLetter` → **produce** the record to `{topic}{DeadLetterTopicSuffix}` (default
    `orders.placed.dead-letter`), then commit the original offset.

## Health check

A cluster-reachability check named `runax:{bus}` is registered automatically for each bus that
runs this transport (`RegisterHealthCheck = false` on the config to opt out). The check requests
cluster metadata through a Kafka admin client.

## Telemetry

The transport reports `messaging.system = "kafka"` on the spans and metrics
emitted by the core package (activity source / meter `"Runax.Messaging"`); the
`messaging.runax.bus` tag identifies the bus.

## License

MIT
