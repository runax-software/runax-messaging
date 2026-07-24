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

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Azure.EventHubs;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddAzureEventHubs(eventHubs =>
    {
        eventHubs.Configure(options =>
        {
            options.FullyQualifiedNamespace = "my-ns.servicebus.windows.net"; // or options.ConnectionString
            options.ConsumerGroup = "orders-worker";
            options.BlobConnectionString = "<Azure Storage connection string>"; // checkpoint store
            options.BlobContainerName = "runax-checkpoints";
        });
        eventHubs.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddAzureEventHubs` lives in the `Runax.Messaging.Transports.Azure.EventHubs` namespace, so add that
`using`. Options are validated at startup; pass an `IConfiguration` section to bind them instead of a
lambda: `AddAzureEventHubs(builder.Configuration.GetSection("EventHubs"))`.

When you set `FullyQualifiedNamespace` (rather than a connection string), the transport authenticates
with `DefaultAzureCredential` (managed identity, environment, Azure CLI, ...).

Provision the event hubs, consumer group, and blob container ahead of time.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `FullyQualifiedNamespace` | `null` | Namespace host (e.g. `my-ns.servicebus.windows.net`); authenticates with `DefaultAzureCredential`. One of this or `ConnectionString` is required. |
| `ConnectionString` | `null` | Namespace connection string. Takes precedence over `FullyQualifiedNamespace` when both are set. |
| `ConsumerGroup` | `$Default` | Consumer group used when subscribing. |
| `BlobConnectionString` | `null` | Azure Storage connection string for the blob checkpoint store. Required to consume. |
| `BlobContainerName` | `null` | Blob container that holds ownership/checkpoint state. Required to consume. |
| `ProduceDeadLetterHub` | `false` | When `true`, a `DeadLetter` verdict republishes the event to a `{topic}.dead-letter` hub (which you must provision); when `false`, dead-lettered events are logged and dropped. |

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

```csharp
builder.Services.AddHealthChecks().AddAzureEventHubsTransport("orders.placed");
```

The check fetches event hub properties for the given probe hub, so it verifies both connectivity and
that the hub exists.

## Telemetry

The transport reports `messaging.system = "azure-event-hubs"` on the spans and metrics emitted by the
core package (activity source / meter `"Runax.Messaging"`).

## License

MIT
