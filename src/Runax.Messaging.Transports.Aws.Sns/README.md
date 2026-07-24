# Runax.Messaging.Transports.Aws.Sns

Amazon SNS transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
SNS is a **fan-out** service, so this transport **publishes to SNS** and **consumes from an SQS queue
subscribed to the topic** (the standard SNS→SQS pattern).

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Aws.Sns
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Aws.Sns;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddSns(sns =>
    {
        sns.Configure(options =>
        {
            options.Region = "us-east-1";
            // consume 'orders.placed' from an SQS queue subscribed to its SNS topic
            options.TopicQueueUrlMap["orders.placed"] =
                "https://sqs.us-east-1.amazonaws.com/123456789012/orders-worker";
        });
        sns.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddSns` lives in the `Runax.Messaging.Transports.Aws.Sns` namespace, so add that `using`.
Options are validated at startup; pass an `IConfiguration` section to bind them instead of a lambda:
`AddSns(builder.Configuration.GetSection("Sns"))`.

You provision the SNS topics, SQS queues, and the SNS→SQS subscriptions (ideally with **raw message
delivery** enabled). Authentication uses the default AWS credential chain unless `AccessKey`/`SecretKey`
are set.

## Options

Configure these on `SnsOptions` via `sns.Configure(o => ...)`.

| Option | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `Region` | AWS region. | `us-east-1` | No |
| `AccessKey` / `SecretKey` | Static credentials; falls back to the default credential chain. | `null` | No |
| `ServiceUrl` | Custom endpoint, e.g. for a local emulator. | `null` | No |
| `TopicArnMap` | Topic → SNS topic ARN for publishing. Unmapped topics are resolved via `CreateTopic` (idempotent). | empty | No |
| `TopicQueueUrlMap` | Topic → SQS queue URL (subscribed to the topic) used to consume it. | empty | To consume a topic |
| `MaxNumberOfMessages` | Messages received per SQS poll (1–10). | `10` | No |
| `WaitTimeSeconds` | SQS long-polling wait time (0–20). | `20` | No |
| `VisibilityTimeoutSeconds` | SQS visibility timeout per receive (0–43200). | `30` | No |

Beyond these transport options, settings from the core package can be applied to this broker
inside the `AddSns(...)` block — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — each overriding the global default for
SNS only. See [Configuration & per-broker settings](../../docs/configuration.md).

## Behavior

- **Publish** sends the serialized envelope to the topic's SNS ARN.
- **Subscribe** long-polls the SQS queue mapped for each topic, unwraps the SNS notification envelope
  (when raw message delivery is off), and maps the dispatch verdict:
  - `Acknowledge` → `DeleteMessage`
  - `Requeue` / `DeadLetter` → leave the message for redelivery / the queue's redrive policy

For heavy fan-in consumption, consume the SQS queue directly with
[`Runax.Messaging.Transports.Aws.Sqs`](../Runax.Messaging.Transports.Aws.Sqs/README.md), which adds
concurrency and visibility-extension controls.

## Health check

```csharp
builder.Services.AddHealthChecks().AddSnsTransport();
```

## Telemetry

The transport reports `messaging.system = "aws_sns"` on the spans and metrics emitted by the core
package (activity source / meter `"Runax.Messaging"`).

## License

MIT
