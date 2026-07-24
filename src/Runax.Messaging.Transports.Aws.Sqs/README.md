# Runax.Messaging.Transports.Aws.Sqs

Amazon SQS transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Topics map to SQS queues (by name, or by an explicit queue-URL mapping).

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Aws.Sqs
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Aws.Sqs;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddSqs(sqs =>
    {
        sqs.Configure(options =>
        {
            options.Region = "us-east-1";
        });
        sqs.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddSqs` lives in the `Runax.Messaging.Transports.Aws.Sqs` namespace, so add that `using`.
Options are validated at startup; pass an `IConfiguration` section to bind them instead of a
lambda: `AddSqs(builder.Configuration.GetSection("Sqs"))`.

## Options

Configure these on `SqsOptions` via `sqs.Configure(o => ...)`.

| Option | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `Region` | AWS region. | `us-east-1` | No |
| `AccessKey` | Access key. If null (with `SecretKey`), the default AWS credential chain is used. | `null` | No |
| `SecretKey` | Secret key. If null (with `AccessKey`), the default AWS credential chain is used. | `null` | No |
| `ServiceUrl` | Custom endpoint, e.g. for LocalStack or testing. | `null` | No |
| `MaxNumberOfMessages` | Maximum messages received per poll (1–10). | `10` | No |
| `WaitTimeSeconds` | Long-polling wait time in seconds (0–20). | `20` | No |
| `MaxConcurrentMessages` | Maximum messages handled concurrently across all polled queues. | `10` | No |
| `VisibilityTimeoutSeconds` | Visibility timeout requested per receive (0–43200), hiding the message while it is processed. | `30` | No |
| `ExtendVisibilityDuringProcessing` | Periodically extend visibility while a message is handled (including retry backoff) so it does not reappear and get processed twice. | `true` | No |
| `TopicQueueUrlMap` | Explicit topic → queue-URL map. Unmapped topics are resolved by queue name via `GetQueueUrl`. | empty | No |

Beyond these transport options, settings from the core package can be applied to this broker
inside the `AddSqs(...)` block — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — each overriding the global default for
SQS only. See [Configuration & per-broker settings](../../docs/configuration.md).

## Behavior

- **Publish** sends the serialized envelope to the resolved queue. **Batch publish**
  (`PublishBatchAsync`) uses `SendMessageBatch`, chunked at the SQS limit of 10 per call.
- **Subscribe** runs one pump per queue in parallel and dispatches messages continuously,
  bounded by `MaxConcurrentMessages` (so successive polls overlap rather than draining one
  batch at a time). Each message maps the dispatch verdict to the queue:
  - `Acknowledge` → `DeleteMessage`
  - `Requeue` and `DeadLetter` → leave the message so its visibility timeout lapses
    and SQS redelivers it, routing to a configured **redrive DLQ** once
    `maxReceiveCount` is reached
- While a message is being handled, the transport extends its visibility timeout
  (`ExtendVisibilityDuringProcessing`) so in-process retry backoff does not let it
  reappear on the queue.

For native dead-lettering, configure a redrive policy on the queue and set
`WithRetry(o => o.Strategy = DeadLetterStrategy.BrokerNative)` in the core registration.

## Health check

Register a reachability check on `IHealthChecksBuilder`:

```csharp
builder.Services.AddHealthChecks().AddSqsTransport();
```

## Telemetry

The transport reports `messaging.system = "sqs"` on the spans and metrics emitted
by the core package (activity source / meter `"Runax.Messaging"`).

## License

MIT
