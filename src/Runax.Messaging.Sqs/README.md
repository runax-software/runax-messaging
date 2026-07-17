# Runax.Messaging.Sqs

Amazon SQS transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Topics map to SQS queues (by name, or by an explicit queue-URL mapping).

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Sqs
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.Sqs;

builder.Services.AddRunaxMessaging(messaging => messaging
    .AddSqs(options =>
    {
        options.Region = "us-east-1";
    })
    .AddConsumer<OrderPlacedConsumer>());
```

`AddSqs` lives in the `Runax.Messaging.Sqs` namespace, so add that `using`.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `Region` | `us-east-1` | AWS region. |
| `AccessKey` | `null` | Access key. If null (with `SecretKey`), the default AWS credential chain is used. |
| `SecretKey` | `null` | Secret key. If null (with `AccessKey`), the default AWS credential chain is used. |
| `ServiceUrl` | `null` | Custom endpoint, e.g. for LocalStack or testing. |
| `MaxNumberOfMessages` | `10` | Maximum messages received per poll. |
| `WaitTimeSeconds` | `20` | Long-polling wait time in seconds. |
| `TopicQueueUrlMap` | empty | Explicit topic → queue-URL map. Unmapped topics are resolved by queue name via `GetQueueUrl`. |

## Behavior

- **Publish** sends the serialized envelope to the resolved queue.
- **Subscribe** long-polls each queue, invokes your consumer, and deletes the
  message on success. A processing failure is logged and the message is left to
  become visible again after the queue's visibility timeout.

## License

MIT
