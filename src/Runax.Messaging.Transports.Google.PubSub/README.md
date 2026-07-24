# Runax.Messaging.Transports.Google.PubSub

Google Cloud Pub/Sub transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
A topic maps to a Pub/Sub topic for publishing and to a subscription for consuming.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Google.PubSub
```

## Register

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Google.PubSub;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddGooglePubSub(pubsub =>
    {
        pubsub.Configure(options =>
        {
            options.ProjectId = "my-gcp-project";
            options.TopicSubscriptionMap["orders.placed"] = "orders-worker"; // topic -> subscription id
        });
        pubsub.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`AddGooglePubSub` lives in the `Runax.Messaging.Transports.Google.PubSub` namespace, so add that `using`.
Options are validated at startup; pass an `IConfiguration` section to bind them instead of a lambda:
`AddGooglePubSub(builder.Configuration.GetSection("PubSub"))`.

Authentication uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
For local development, point at the Pub/Sub emulator with the `PUBSUB_EMULATOR_HOST` environment variable.

## Options

Configure these on `GooglePubSubOptions` via `pubsub.Configure(o => ...)`.

| Option | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `ProjectId` | Google Cloud project id. | (none) | Yes |
| `TopicSubscriptionMap` | Topic → subscription id used to consume it. Topics without an entry consume from a subscription named after the topic. | empty | No |

Beyond these transport options, settings from the core package can be applied to this broker
inside the `AddGooglePubSub(...)` block — `AddConsumer<T>()`, `WithRetry(...)`,
`OnUnroutableMessage(...)`, `ConfigureSerialization(...)`, and `UseSerializer<T>()` — each
overriding the global default for Pub/Sub only. See
[Configuration & per-broker settings](../../docs/configuration.md).

## Behavior

- **Publish** sends the serialized envelope to the Pub/Sub topic named after the runax topic.
- **Subscribe** starts a streaming pull per topic (on the mapped subscription) and maps the dispatch
  verdict to the message:
  - `Acknowledge` → ack
  - `Requeue` and `DeadLetter` → nack, so Pub/Sub redelivers — and, once `maxDeliveryAttempts` is
    reached, routes the message to the subscription's **dead-letter topic** if one is configured

Topics and subscriptions are **not** created by the transport; provision them (and any dead-letter
policy) ahead of time.

## Health check

Register a reachability check on `IHealthChecksBuilder`:

```csharp
builder.Services.AddHealthChecks().AddGooglePubSubTransport();
```

## Telemetry

The transport reports `messaging.system = "google_pubsub"` on the spans and metrics emitted by the
core package (activity source / meter `"Runax.Messaging"`).

## License

MIT
