# Runax.Messaging.Transports.Google.PubSub

Google Cloud Pub/Sub transport for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
A topic maps to a Pub/Sub topic for publishing and to a subscription for consuming.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Transports.Google.PubSub
```

## Register

Attach the transport to a bus with its config object:

```csharp
using Runax.Messaging;
using Runax.Messaging.Transports.Google.PubSub;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new GooglePubSubConfig
        {
            ProjectId = "my-gcp-project",
            TopicSubscriptionMap =
            {
                ["orders.placed"] = "orders-worker",   // topic -> subscription id
            },
        });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

`GooglePubSubConfig` lives in the `Runax.Messaging.Transports.Google.PubSub` namespace, so add that
`using`. The config is validated at startup (DataAnnotations, when the `AddBus` block completes);
bind it from an `IConfiguration` section instead of setting properties:
`bus.AddTransport<GooglePubSubConfig>(builder.Configuration.GetSection("PubSub"))`.

Authentication uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
For local development, point at the Pub/Sub emulator with the `PUBSUB_EMULATOR_HOST` environment variable.

## Config

Set these directly on `GooglePubSubConfig`.

| Property | Meaning | Default | Required? |
| --- | --- | --- | --- |
| `ProjectId` | Google Cloud project id. | (none) | Yes |
| `TopicSubscriptionMap` | Topic → subscription id used to consume it. Topics without an entry consume from a subscription named after the topic. | empty | No |
| `RegisterHealthCheck` | Auto-register the `runax:{bus}` health check (inherited from `TransportConfig`). | `true` | No |

Beyond the transport config, settings from the core package apply to the bus that runs this
transport — `AddConsumer<T>()`, `WithRetry(...)`, `OnUnroutableMessage(...)`,
`ConfigureSerialization(...)`, and `UseSerializer<T>()` — all inside the same `AddBus` block.
See [Configuration & per-bus settings](../../docs/configuration.md).

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

A reachability check named `runax:{bus}` is registered automatically for each bus that runs
this transport (`RegisterHealthCheck = false` on the config to opt out).

## Telemetry

The transport reports `messaging.system = "google_pubsub"` on the spans and metrics emitted by the
core package (activity source / meter `"Runax.Messaging"`); the `messaging.runax.bus` tag identifies
the bus.

## License

MIT
