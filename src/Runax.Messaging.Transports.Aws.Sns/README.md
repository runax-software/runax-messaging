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

builder.Services.AddRunaxMessaging(messaging => messaging
    .AddSns(options =>
    {
        options.Region = "us-east-1";
        // consume 'orders.placed' from an SQS queue subscribed to its SNS topic
        options.TopicQueueUrlMap["orders.placed"] =
            "https://sqs.us-east-1.amazonaws.com/123456789012/orders-worker";
    })
    .AddConsumer<OrderPlacedConsumer>());
```

`AddSns` lives in the `Runax.Messaging.Transports.Aws.Sns` namespace, so add that `using`.
Options are validated at startup; pass an `IConfiguration` section to bind them instead of a lambda:
`AddSns(builder.Configuration.GetSection("Sns"))`.

You provision the SNS topics, SQS queues, and the SNS→SQS subscriptions (ideally with **raw message
delivery** enabled). Authentication uses the default AWS credential chain unless `AccessKey`/`SecretKey`
are set.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `Region` | `us-east-1` | AWS region. |
| `AccessKey` / `SecretKey` | `null` | Static credentials; falls back to the default credential chain. |
| `ServiceUrl` | `null` | Custom endpoint, e.g. for a local emulator. |
| `TopicArnMap` | empty | Topic → SNS topic ARN for publishing. Unmapped topics are resolved via `CreateTopic` (idempotent). |
| `TopicQueueUrlMap` | empty | Topic → SQS queue URL (subscribed to the topic) used to consume it. Required to consume a topic. |
| `MaxNumberOfMessages` | `10` | Messages received per SQS poll. |
| `WaitTimeSeconds` | `20` | SQS long-polling wait time. |
| `VisibilityTimeoutSeconds` | `30` | SQS visibility timeout per receive. |

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
