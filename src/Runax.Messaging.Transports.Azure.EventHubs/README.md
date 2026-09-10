# Runax.Messaging.Transports.Azure.EventHubs

Azure Event Hubs transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
A runax **topic maps to an event hub of the same name**: publishing sends to that hub, and consuming
runs an `EventProcessorClient` over a consumer group backed by a blob checkpoint store.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Azure.EventHubs
```

## Register

Attach the transport to a bus with its config object:

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Azure.EventHubs;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new AzureEventHubsConfig
        {
            FullyQualifiedNamespace = "my-ns.servicebus.windows.net", // or ConnectionString
            ConsumerGroup = "orders-worker",
            BlobConnectionString = "<Azure Storage connection string>", // checkpoint store
            BlobContainerName = "runax-checkpoints",
        });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AzureEventHubsConfig` lives in the `Runax.Messaging.Transports.Azure.EventHubs` namespace, so add
that `using`. The config is validated at startup (DataAnnotations, when the `AddBus` block
completes); bind it from an `IConfiguration` section instead of setting properties:
`bus.AddTransport<AzureEventHubsConfig>(builder.Configuration.GetSection("EventHubs"))`.

When you set `FullyQualifiedNamespace` (rather than a connection string), the transport authenticates
with `DefaultAzureCredential` (managed identity, environment, Azure CLI, ...).

Provision the event hubs, consumer group, and blob container ahead of time.

## Config

Set these directly on `AzureEventHubsConfig`.

| Property | Default | Description |
| --- | --- | --- |
| `FullyQualifiedNamespace` | `null` | Namespace host (e.g. `my-ns.servicebus.windows.net`); authenticates with `DefaultAzureCredential`. One of this or `ConnectionString` is required. |
| `ConnectionString` | `null` | Namespace connection string. Takes precedence over `FullyQualifiedNamespace` when both are set. |
| `ConsumerGroup` | `$Default` | Consumer group used when subscribing. |
| `BlobConnectionString` | `null` | Azure Storage connection string for the blob checkpoint store. Required to consume. |
| `BlobContainerName` | `null` | Blob container that holds ownership/checkpoint state. Required to consume. |
| `ProduceDeadLetterHub` | `false` | When `true`, a `DeadLetter` verdict republishes the event to a `{topic}.dead-letter` hub (which you must provision); when `false`, dead-lettered events are logged and dropped. |
| `RegisterHealthCheck` | `true` | Auto-register the `runax:{bus}` health check (inherited from `TransportConfig`). |

Beyond the transport config, settings from the core package apply to the bus that runs this
transport — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — all inside the same `AddBus` block.
See [Configuration & per-bus settings](../../docs/configuration.md).

## Behavior

- **Publish** sends the serialized envelope to the event hub named after the runax topic (via
  `EventHubProducerClient`). `PublishBatchAsync` packs events into size-bounded `EventDataBatch`es.
- **Subscribe** runs an `EventProcessorClient` per topic over the configured consumer group, using a
  `BlobCheckpointStore` for ownership and checkpoints. Event Hubs has **no per-message ack and no native
  dead-letter queue**, so the dispatch verdict is mapped to checkpointing:
  - `Acknowledge` → advance the checkpoint past the event.
  - `Requeue` → **do not** checkpoint, so the partition is reprocessed from the last committed offset
    (by this run's next poll or another owner).
  - `DeadLetter` → if `ProduceDeadLetterHub` is `true`, republish the event to a `{topic}.dead-letter`
    hub and checkpoint; otherwise log and checkpoint (drop). Either way the event is not reprocessed.

## Health check

A check named `runax:{bus}` is registered automatically for each bus that runs this transport
(`RegisterHealthCheck = false` on the config to opt out). The check pings the Event Hubs
namespace to verify connectivity.

## Telemetry

The transport reports `messaging.system = "azure-event-hubs"` on the spans and metrics emitted by the
core package (activity source / meter `"Runax.Messaging"`); the `messaging.runax.bus` tag identifies
the bus.

## License

MIT
