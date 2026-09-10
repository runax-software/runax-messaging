# Consumers & the reliability pipeline

Consuming in Runax.Messaging is built around one base class and one hosted dispatcher. You
write a `MessageConsumer<TMessage>` per topic and register it on a bus with
`bus.AddConsumer<T>()`; the framework runs a background dispatcher per consuming bus that
subscribes to the registered topics and wraps every delivery in a uniform, transport-agnostic
reliability pipeline — contract-version routing, retries with exponential backoff, poison
handling, and dead-lettering. This page walks that pipeline end to end: what you write, what
the framework does around it, and every knob that changes the outcome.

For where consuming sits in the overall design, see
[Architecture & message flow](architecture.md); for the scoping model behind the policies used
here (`WithRetry`, `OnUnroutableMessage`, ...), see
[Configuration & per-bus settings](configuration.md).

## Writing a consumer

A consumer derives from `MessageConsumer<TMessage>`, names its topic, and implements
`HandleAsync`. The framework deserializes the message body to `TMessage` before invoking you —
by the time `HandleAsync` runs, you hold a typed payload, not JSON:

```csharp
public sealed record OrderPlaced(Guid OrderId, decimal Total);

public sealed class OrderPlacedConsumer(IOrderProjection projection) : MessageConsumer<OrderPlaced>
{
    public override string Topic => "orders.placed";

    protected override async ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken = default)
    {
        await projection.ApplyAsync(message.OrderId, message.Total, cancellationToken);
    }
}
```

- `Topic` is the subscription: the bus's dispatcher subscribes to the union of its consumers'
  topics at startup.
- `cancellationToken` is signaled when the host shuts down — honor it in long-running work.
- Deserialization runs through the received message's `MessageContext` using the same
  serializer selection as publishing (per-topic, then per-bus, then default — see
  [Serialization & custom serializers](serialization.md)). The `MessageContext` itself — raw
  `Body`, `Headers`, `ContractName` / `ContractVersion` — is the framework's dispatch carrier;
  the typed base class deliberately hands you only the payload. If the body deserializes to
  `null`, the consumer throws an `InvalidOperationException`, which flows through the normal
  retry-then-dead-letter path below.

Registration happens inside the bus block:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

### Consumers are singletons

`AddConsumer<T>()` registers the consumer as a **singleton** — one instance is created at
startup and handles every message for as long as the host runs. Two rules follow:

- **Keep consumers stateless.** Constructor-injected dependencies must be singletons or
  thread-safe; do not accumulate per-message state in fields. On transports that dispatch
  concurrently (e.g. SQS with `MaxConcurrentMessages` > 1), `HandleAsync` runs on the same
  instance from several messages at once.
- **One instance can serve several buses.** Registering the same consumer type on two buses
  (`bus.AddConsumer<T>()` in each block) subscribes that single instance on both transports;
  each bus's traffic still flows through that bus's own retry and dead-letter policy.

## The dispatch pipeline, end to end

Every delivered message runs through the same pipeline in the bus's dispatcher
(`MessageConsumerHostedService`), which ends by returning a `MessageDisposition` for the
transport to apply:

```
transport delivers envelope JSON for a subscribed topic
        │
        ├─ 1. deserialize envelope → MessageContext
        │        └─ malformed → dead-letter immediately (x-runax-dlq-attempts: 0)
        │
        ├─ 2. match the topic's consumers against the envelope's contract version
        │        └─ none match → the bus's IUnroutableMessageHandler decides
        │                        (dead-letter by default — see "Unroutable messages")
        │
        ├─ 3. dispatch to each matched consumer in turn
        │        └─ per consumer: retry loop with backoff → on exhaustion / poison → dead-letter
        │
        └─ 4. merge the per-consumer results into one MessageDisposition
                 └─ transport maps it to a broker action (ack / requeue / reject)
```

The three disposition values mean:

| Disposition | Meaning | Broker effect |
| --- | --- | --- |
| `Acknowledge` | Handled successfully — or already dead-lettered by the framework, or deliberately dropped. | Remove the message. |
| `Requeue` | Could not be safely handled right now. | Return it for redelivery (RabbitMQ `basic.nack` with requeue, SQS visibility timeout, in-memory re-enqueue). |
| `DeadLetter` | Must not be redelivered. | Reject it toward the broker's native dead-letter mechanism; transports without one discard it. |

### Several consumers on one topic

A topic can have several matched consumers (for example a versioned consumer plus an
unversioned accept-all consumer). They are dispatched **sequentially**, each with its own full
retry loop, and their individual results merge into a single disposition with two exact rules:

- **`Requeue` wins outright.** The first consumer whose dispatch resolves to `Requeue` ends the
  pipeline immediately — remaining consumers are not invoked for this delivery — and the whole
  message is redelivered. Consumers that already succeeded will see the message **again** on
  redelivery, which is one more reason handlers must be idempotent.
- **One `DeadLetter` escalates.** A `DeadLetter` result is recorded but the remaining consumers
  still run; if any consumer's dispatch resolved to `DeadLetter`, the merged result is
  `DeadLetter` instead of `Acknowledge`.

Note that under the default `DeadLetterStrategy.FrameworkManaged`, a consumer failure resolves
to `Acknowledge` (the framework has already republished the message to the dead-letter topic),
so the merged result stays `Acknowledge`. A per-consumer `DeadLetter` result — and therefore a
merged `DeadLetter` — occurs with `DeadLetterStrategy.BrokerNative`; a `Requeue` result occurs
when a backoff wait is cancelled by shutdown or when a framework dead-letter publish itself
fails (both below).

## Contract versioning

Versioning is opt-in via `[MessageContract(version)]` on the message type (see the
[architecture page](architecture.md#contract-versioning) for the envelope format). On the
consume side it drives step 2 of the pipeline, with one matching rule:

> A consumer whose message type carries `[MessageContract(v)]` accepts **only** messages whose
> envelope carries version `v`. A consumer whose message type has no attribute is
> **unversioned** and accepts **every** message on its topic.

Consequences worth spelling out:

- A versioned consumer does **not** receive unversioned messages — `null` is not a version
  match. Mixing unversioned producers with only-versioned consumers makes every message
  unroutable.
- An unversioned consumer receives all versions, deserialized into its single type — fine when
  the type is forward-compatible with every version on the wire, wrong otherwise.
- A message can match several consumers at once (its exact version's consumer plus any
  unversioned one); the merge rules above apply.

### Evolving a contract side by side

The intended pattern for evolving a contract is one consumer per version on the same topic.
Each consumer receives exactly its own shape at full fidelity — no `JsonElement` sniffing, no
optional properties:

```csharp
[MessageContract(1)]
public sealed record OrderPlacedV1(Guid OrderId, decimal Total);

[MessageContract(2)]
public sealed record OrderPlacedV2(Guid OrderId, decimal Total, string Currency);

public sealed class OrderPlacedV1Consumer(IOrderProjection projection) : MessageConsumer<OrderPlacedV1>
{
    public override string Topic => "orders.placed";

    protected override async ValueTask HandleAsync(OrderPlacedV1 message, CancellationToken cancellationToken = default)
    {
        await projection.ApplyAsync(message.OrderId, message.Total, "USD", cancellationToken);
    }
}

public sealed class OrderPlacedV2Consumer(IOrderProjection projection) : MessageConsumer<OrderPlacedV2>
{
    public override string Topic => "orders.placed";

    protected override async ValueTask HandleAsync(OrderPlacedV2 message, CancellationToken cancellationToken = default)
    {
        await projection.ApplyAsync(message.OrderId, message.Total, message.Currency, cancellationToken);
    }
}
```

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedV1Consumer>();   // keeps handling in-flight V1 traffic
        bus.AddConsumer<OrderPlacedV2Consumer>();   // handles the new shape
    });
});
```

Deploy the V2 consumer *before* any producer starts emitting V2 — `IMessageContractCatalog`
(`Handled`, `Accepts(topic, version)`) lets a producer verify coverage at startup. Once V1
traffic drains, delete the V1 type and consumer.

## Unroutable messages

A message is **unroutable** when it arrives on a subscribed topic but no registered consumer on
that bus accepts its contract version — typically a version this application does not (yet)
handle, per the matching rule above. The bus's `IUnroutableMessageHandler` then decides its
fate; the built-in strategies are selected with `bus.OnUnroutableMessage(...)`:

| Strategy | Behavior |
| --- | --- |
| `UnroutableStrategy.DeadLetter` | **Default.** Route the message through the bus's dead-letter strategy (below), so nothing is silently dropped. The `x-runax-dlq-reason` header records the unaccepted version, and `x-runax-dlq-attempts` is `0` — the message never reached a consumer. |
| `UnroutableStrategy.Requeue` | Redeliver the message. **Use with care:** if no consumer for that version ever appears, this loops forever — it is a "the deploy is minutes away" strategy, not a steady state. |
| `UnroutableStrategy.Discard` | Acknowledge and drop the message. |

For anything else, register a custom handler. It receives an `UnroutableMessage` — topic, raw
JSON `Body`, `Headers`, `ContractName` / `ContractVersion`, and the delivering transport's
`SystemName` — and returns the disposition to apply to the original message. A common pattern
is forwarding to a quarantine topic on a bus that can publish:

```csharp
public sealed class QuarantineUnroutableHandler(
    IBus bus,
    ILogger<QuarantineUnroutableHandler> logger) : IUnroutableMessageHandler
{
    public async ValueTask<MessageDisposition> HandleAsync(
        UnroutableMessage message, CancellationToken cancellationToken = default)
    {
        logger.LogWarning(
            "Quarantining unroutable message from '{Topic}' (contract version {Version}).",
            message.Topic, message.ContractVersion);

        await bus.PublishAsync("quarantine", message, cancellationToken);
        return MessageDisposition.Acknowledge;
    }
}
```

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedV1Consumer>();
        bus.OnUnroutableMessage<QuarantineUnroutableHandler>();
    });
});
```

Returning `MessageDisposition.DeadLetter` from a custom handler routes through the configured
dead-letter strategy exactly like the built-in default; `Requeue` redelivers; `Acknowledge`
discards. The handler is bus-scoped: each bus configures its own via `OnUnroutableMessage`, and
a bus without one uses the built-in `DeadLetter` default. There is no per-topic form.

## Retries

A consumer that throws (anything other than `PoisonMessageException`) is retried in place with
exponential backoff, governed by `RetryOptions`:

| Property | Default | Meaning |
| --- | --- | --- |
| `MaxAttempts` | `3` | Total invocations per consumer per message — the initial attempt plus retries. Must be ≥ 1. |
| `InitialDelay` | `1 second` | Delay before the first retry. |
| `BackoffFactor` | `2.0` | Exponential multiplier applied per retry. Must be ≥ 1.0. |
| `MaxDelay` | `30 seconds` | Upper bound on any single delay. |
| `EnableDeadLettering` | `true` | Whether exhausted / poison messages are dead-lettered; `false` logs and drops them. |
| `DeadLetterTopicSuffix` | `".dead-letter"` | Suffix forming the dead-letter topic under `FrameworkManaged`. |
| `Strategy` | `DeadLetterStrategy.FrameworkManaged` | How dead-lettering is performed (see below). |

The delay after failed attempt *n* is exactly:

```
delay(n) = min(InitialDelay × BackoffFactor^(n − 1), MaxDelay)
```

With the defaults that is 1 s after the first failure and 2 s after the second; the third
failure exhausts `MaxAttempts` and the message is dead-lettered. With
`MaxAttempts = 8` the sequence is 1 s, 2 s, 4 s, 8 s, 16 s, 30 s, 30 s — the cap flattens the
curve. The retry loop runs **per consumer**: with several matched consumers on a topic, each
gets its own attempts.

### Cancellation

Shutdown interacts with the retry loop in two precise ways:

- A failure when cancellation has already been requested is **not retried** — it goes straight
  to the dead-letter path, regardless of remaining attempts.
- A cancellation that fires **during a backoff delay** returns `Requeue`, so the broker
  redelivers the message after the application restarts.

### Policy scoping and precedence

Retry policy resolves per `(bus, topic)`, most specific first, and the result is cached:

1. `bus.WithRetryForTopic("<topic>", o => ...)` — the policy for this exact topic on this bus;
2. `bus.WithRetry(o => ...)` — the bus-wide policy;
3. the built-in `RetryOptions` defaults above.

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport<KafkaConfig>(c => c.BootstrapServers = "localhost:9092");
        bus.AddConsumer<PaymentConsumer>();
        bus.AddConsumer<TelemetryConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 5);                        // any topic on this bus
        bus.WithRetryForTopic("payments", o => o.MaxAttempts = 10);   // "payments" only
        bus.WithRetryForTopic("telemetry", o => o.EnableDeadLettering = false);
    });

    messaging.AddBus("audit", bus =>
    {
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
        bus.AddConsumer<PaymentConsumer>();
        // no WithRetry here -> built-in defaults (MaxAttempts 3, ...)
    });
});
```

One subtlety worth internalizing: a per-topic policy starts from the **`RetryOptions`
defaults** with your action applied on top — it does *not* layer on the bus-wide policy. In the
example above, `"telemetry"` has `MaxAttempts = 3` (the default), not the bus's `5`; set every
property you care about in the per-topic action. Policies never cross buses: the same consumer
type registered on two buses runs under each bus's own policy.

## Poison messages

Retrying only helps transient failures. When a consumer can tell a message will **never**
succeed — a business-rule violation, a permanently invalid reference — it throws
`PoisonMessageException`, and the pipeline skips all remaining retry attempts and dead-letters
the message immediately:

```csharp
protected override async ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken = default)
{
    var customer = await customers.FindAsync(message.OrderId, cancellationToken);
    if (customer is null)
        throw new PoisonMessageException($"Order {message.OrderId} references an unknown customer.");

    await projection.ApplyAsync(message.OrderId, message.Total, cancellationToken);
}
```

The exception's message becomes the `x-runax-dlq-reason` header, and `x-runax-dlq-attempts`
records the attempt on which the consumer gave up (usually `1`). Constructors take an optional
inner exception to preserve the underlying cause.

## Dead-lettering

Dead-lettering fires for four causes: a malformed envelope, an unroutable message whose handler
returned `DeadLetter`, a `PoisonMessageException`, and retry exhaustion. What happens next is
decided by the resolved `RetryOptions` for that `(bus, topic)`:

### `DeadLetterStrategy.FrameworkManaged` (default)

The pipeline republishes the **original envelope, verbatim** to
`{topic}{DeadLetterTopicSuffix}` (default: `orders.placed` → `orders.placed.dead-letter`) on
the same transport, enriched with diagnostic headers, then acknowledges the original. It works
on every transport with no broker-side setup.

| Header | Value |
| --- | --- |
| `x-runax-dlq-reason` | The exception message — the consumer's failure, the poison reason, the envelope parse error, or the unaccepted contract version. |
| `x-runax-dlq-exception` | The exception's full type name. |
| `x-runax-dlq-original-topic` | The topic the message was delivered on. |
| `x-runax-dlq-attempts` | How many times a consumer was invoked. `0` means the message never reached a consumer (malformed envelope or unroutable). |
| `x-runax-dlq-timestamp` | When it was dead-lettered — UTC, ISO 8601 round-trip format. |

Because the payload and existing headers are preserved, a redrive tool can strip the
`x-runax-dlq-*` headers and republish to `x-runax-dlq-original-topic` once the underlying
problem is fixed.

If the dead-letter publish **itself** fails (broker outage, shutdown mid-publish), the pipeline
returns `Requeue` — the message goes back to the broker for redelivery rather than being lost.

### `DeadLetterStrategy.BrokerNative`

The pipeline returns `MessageDisposition.DeadLetter` and lets the broker's own facility take
over — a RabbitMQ dead-letter exchange, an SQS redrive policy. This requires broker-side
configuration, and **transports without a native dead-letter mechanism discard the message**;
see each transport's package README for how it maps the disposition. Pair `BrokerNative` with
`MaxAttempts = 1` to rely purely on the broker for retries (e.g. SQS `maxReceiveCount`),
avoiding two multiplied retry layers.

### `EnableDeadLettering = false`

The message is logged with a warning and **dropped** (acknowledged). Reserve this for streams
where losing a failed message is genuinely acceptable — high-volume telemetry, not commands.

### Dead-lettering on a `ConsumeOnly` bus

`BusMode.ConsumeOnly` blocks the *application* publishing surface (`bus.PublishAsync` throws);
the framework's own dead-letter publish is part of the consume pipeline and **remains
allowed**. If your broker credentials genuinely cannot write, `FrameworkManaged` will fail at
runtime (and requeue, per the failure rule above) — use `DeadLetterStrategy.BrokerNative` or
`EnableDeadLettering = false` on that bus instead.

## Hosting

Consuming requires a .NET Generic Host. Each consuming bus gets its **own**
`MessageConsumerHostedService`, registered automatically — a `PublishOnly` bus starts none:

- **Subscribe at startup.** The service resolves the bus's registered consumers, groups them by
  `Topic`, and calls the transport's `SubscribeAsync` once for the whole topic set. A consuming
  bus with no registered consumers logs an informational message and idles. Consumer
  constructors run here — a failing constructor fails the bus at startup, not on first message.
- **Graceful shutdown.** Host shutdown signals the stopping token: in-flight handlers receive
  it as their `cancellationToken`, backoff waits abort with `Requeue`, and the transport's
  subscription winds down.
- **Bus independence.** Buses subscribe, run, and shut down independently. A transport failure
  on one bus — a broker down, a subscription fault — does not touch the other buses' dispatch
  loops, policies, or health.

Everything the pipeline does is observable: a `Consumer` span per dispatch (linked to the
producer's trace context), `runax.messaging.consumed` / `failed` counters, and a
`runax.messaging.processing.duration` histogram, all tagged with the bus name. See
[Observability](observability.md).

## See also

- [Buses](buses.md) — the bus model, modes, and multi-bus topologies.
- [Publishing](publishing.md) — the other half of the pipeline.
- [Serialization & custom serializers](serialization.md) — how bodies and the `__runax` envelope are encoded.
- [Testing](testing.md) — exercising consumers and the reliability pipeline with the in-memory transport.
- [Observability](observability.md) — the spans, metrics, and health checks emitted by the dispatcher.
- Each transport's package README for how it maps `MessageDisposition` to broker actions.
