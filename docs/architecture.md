# Architecture & message flow

Runax.Messaging separates the messaging *contract* from its *implementation* and
from each *transport*, so applications depend on stable abstractions while
brokers stay swappable.

## Package layering

```
Runax.Messaging.Abstractions      (contracts: IMessagePublisher, IMessagingTransport, MessageContext, MessagingConfigurator)
        ▲                 ▲
        │                 │
Runax.Messaging      Runax.Messaging.Sqs / .RabbitMq / <your transport>
(impl + in-memory)   (implement IMessagingTransport, add a configurator extension)
```

- **Abstractions** depends on nothing but `Microsoft.Extensions.DependencyInjection.Abstractions`.
- **Core** references Abstractions and provides serialization, the publisher
  adapter, the hosted consumer dispatcher, and the in-memory transport.
- **Transports** reference Abstractions only — never each other, and never the
  core implementation.

## Key types

| Type | Package | Responsibility |
| --- | --- | --- |
| `IMessagePublisher` | Abstractions | The publish API applications call. |
| `IMessagingTransport` | Abstractions | Provider SPI: broker-specific publish/subscribe over serialized envelopes. |
| `MessageContext` | Abstractions | A received message (topic, body, headers) with `Deserialize<T>()`. |
| `MessagingConfigurator` | Abstractions | Fluent builder; transports and consumers attach via extensions. |
| `MessagePublisherAdapter` | Core (internal) | Bridges `IMessagePublisher` → `IMessagingTransport`, serializing to an envelope. |
| `IMessageSerializer` / `JsonMessageSerializer` | Core (internal) | Envelope serialization (System.Text.Json). |
| `MessageConsumer<TMessage>` | Core | Base class for a typed consumer of a single topic. |
| `MessageConsumerHostedService` | Core (internal) | Background service that subscribes consumers and dispatches messages. |

## The envelope

Messages travel wrapped in an envelope so metadata rides alongside the payload:

```json
{
  "MessageType": "MyApp.Order, MyApp",
  "Body": "{\"Id\":1,\"Name\":\"widget\"}",
  "Headers": { "correlation-id": "abc" }
}
```

`Body` is the JSON-serialized message; `Headers` carries transport-level
metadata. The transport only ever sees the serialized envelope string — it never
needs to know your message types.

## Publish flow

```
publisher.PublishAsync(topic, message[, headers])
        └─ MessagePublisherAdapter: serialize (message + headers) → envelope JSON
                └─ IMessagingTransport.PublishAsync(topic, envelopeJson)
                        └─ broker (queue / exchange / channel)
```

## Consume flow

```
Host starts → MessageConsumerHostedService.ExecuteAsync
        ├─ resolve each registered consumer, group by Topic
        └─ IMessagingTransport.SubscribeAsync(topics, onMessage)
                └─ per message: deserialize envelope → MessageContext
                        └─ MessageConsumer<T>: deserialize Body → T → HandleAsync(T)
```

Consumers are ordinary DI singletons. Dispatch runs inside a hosted
`BackgroundService`, so consuming requires a .NET Generic Host. Publishing has no
such requirement.

## Error handling

Each transport decides how a failed `HandleAsync` is treated (see the transport's
package README):

- **SQS** — the message is not deleted, so it reappears after the visibility
  timeout.
- **RabbitMQ** — the message is nacked with requeue.
- **In-memory** — no redelivery; the exception surfaces to the subscribe loop.

## Design rules

- Transports depend on **Abstractions only**. Keep broker SDKs out of the core.
- Exactly one transport is registered per configuration.
- Applications depend on `IMessagePublisher` (from Abstractions), not on any
  concrete transport.
