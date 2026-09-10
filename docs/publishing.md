# Publishing

How to publish messages: getting hold of a bus, topics and headers, batching, what the
publish pipeline actually does, and how the outbox changes delivery without changing your
code. For the consuming side see [Consuming](consuming.md); for buses themselves see
[Buses](buses.md).

## The one abstraction

All publishing goes through `IBus` — there is no separate publisher type, no per-message
client, and application code never touches a transport. The interface lives in
`Runax.Messaging.Abstractions`, so a project that only publishes depends on nothing else:

```csharp
public interface IBus
{
    string Name { get; }
    BusMode Mode { get; }

    ValueTask PublishAsync<TMessage>(string topic, TMessage message,
        CancellationToken cancellationToken = default);

    ValueTask PublishAsync<TMessage>(string topic, TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default);

    ValueTask PublishBatchAsync<TMessage>(string topic, IReadOnlyList<TMessage> messages,
        CancellationToken cancellationToken = default);
}
```

Three things to internalize about these signatures:

- **Completion means "handed to the transport."** The returned task completes once the
  serialized envelope has been accepted by the broker (or written to the outbox store —
  see [below](#publishing-through-the-outbox)). It does **not** mean a consumer has seen
  the message. Delivery is **at-least-once** with no deduplication: a retry after an
  ambiguous failure can put the same message on the broker twice, so keep consumers
  idempotent.
- **`ValueTask`, not `Task`.** Publishes can complete synchronously (the in-memory
  transport, an in-memory outbox store), so the API avoids a `Task` allocation on that
  path. The usual `ValueTask` rules apply: await it exactly once, immediately; don't
  store it, await it twice, or block on it. If you need `Task` semantics (e.g. for
  `Task.WhenAll`), call `.AsTask()`.
- **A `ConsumeOnly` bus throws.** Publishing is guarded by the bus's declared `BusMode`
  before anything is serialized — see [the recipe](#publishing-on-a-consumeonly-bus-throws).

A minimal publish, with a realistic message type:

```csharp
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total);

public sealed class CheckoutService(IBus bus)
{
    public async Task CompleteCheckoutAsync(Order order, CancellationToken ct)
    {
        // ... persist the order ...
        await bus.PublishAsync(
            "orders.placed",
            new OrderPlaced(order.Id, order.CustomerId, order.Total),
            ct);
    }
}
```

The message type must serialize to a **JSON object** (arrays and primitives as top-level
messages throw at publish time), and it may not declare a property named `__runax` — that
key is reserved for the envelope. See [Serialization](serialization.md).

## Getting a bus

A bus is registered with `AddBus` (see [Buses](buses.md)); publishing code only decides
*which* registered bus to talk to. Three ways, most specific need last:

**Unkeyed injection — the default bus.** An unkeyed `IBus` resolves to the default bus
(the one registered by the parameterless `AddBus`), or, when only a single named bus
exists, to that bus. This is the right choice for almost all application code, and it
keeps the code dependent on `Runax.Messaging.Abstractions` alone:

```csharp
public sealed class CheckoutService(IBus bus) { /* ... */ }
```

**Keyed injection — a named bus.** When the application runs several buses, ask for one
by name with `[FromKeyedServices]`:

```csharp
public sealed class AuditWriter([FromKeyedServices("audit")] IBus auditBus)
{
    public ValueTask RecordAsync(AuditEntry entry, CancellationToken ct) =>
        auditBus.PublishAsync("audit.entries", entry, ct);
}
```

**`IBusProvider` — dynamic lookup.** When the bus is chosen at runtime (per tenant, per
feature flag) or you need to enumerate every bus (diagnostics, admin surfaces):

```csharp
public sealed class TenantEventPublisher(IBusProvider buses)
{
    public ValueTask PublishAsync(string tenantBus, OrderPlaced message, CancellationToken ct) =>
        buses.GetBus(tenantBus).PublishAsync("orders.placed", message, ct);
}
```

`GetBus` throws `InvalidOperationException` for an unregistered name; `Buses` lists every
configured bus in registration order.

| You are writing... | Inject | Why |
| --- | --- | --- |
| Ordinary application code, one bus (or clearly one "main" bus) | `IBus` (unkeyed) | Simplest; depends on Abstractions only. |
| Code bound to a specific named bus | `[FromKeyedServices("name")] IBus` | The binding is explicit and fails fast at resolution. |
| Code that picks a bus at runtime | `IBusProvider` | `GetBus(name)` per call. |
| Diagnostics / admin over all buses | `IBusProvider` | `Buses` enumerates them. |

When several buses exist and none is the default, the unkeyed `IBus` is ambiguous and
throws at resolution — inject a keyed `IBus` or use the provider instead.

## Topics

A topic is a plain string; the convention is **dotted lowercase**, most-general segment
first: `orders.placed`, `orders.payment.captured`, `inventory.stock.depleted`. Stick to
lowercase letters, digits, dots, and dashes — every broker accepts that alphabet.

What a topic physically maps to is the transport's business, not yours:

| Transport | A topic becomes... |
| --- | --- |
| RabbitMQ | the routing key on a topic exchange |
| Kafka | a Kafka topic of the same name |
| AWS SQS | a queue (by name, or an explicit topic → queue-URL map) |
| AWS SNS | an SNS topic (consumed via a subscribed SQS queue) |
| Azure Service Bus | a Service Bus topic |
| Azure Event Hubs | an event hub of the same name |
| Google Pub/Sub | a Pub/Sub topic |
| Redis | a stream key |

The exact mapping — including per-topic overrides such as SQS's `TopicQueueUrlMap` — is
documented in each transport's README (e.g.
[RabbitMQ](../src/Runax.Messaging.Transports.RabbitMq/README.md),
[SQS](../src/Runax.Messaging.Transports.Aws.Sqs/README.md),
[Kafka](../src/Runax.Messaging.Transports.Kafka/README.md)). If you are building a
transport, see [Writing a custom transport](writing-a-custom-transport.md).

## Headers

The second `PublishAsync` overload attaches string key/value headers to the message:

```csharp
await bus.PublishAsync(
    "orders.placed",
    new OrderPlaced(order.Id, order.CustomerId, order.Total),
    new Dictionary<string, string>
    {
        ["correlation-id"] = correlationId,
        ["source"] = "checkout-api",
    },
    ct);
```

Rules of the road:

- **Headers ride in the envelope**, under `__runax.headers` — not in broker-native
  message attributes. That makes them identical on every transport, and consumers read
  them back from `MessageContext.Headers` regardless of broker.
- **Your dictionary is copied**, never mutated: reuse it freely across publishes.
- **W3C trace context is injected automatically.** When a trace is active, the bus
  injects the current context (`traceparent`, `tracestate`, and baggage, per
  `DistributedContextPropagator.Current`) into the outgoing headers so the consumer's
  span joins your trace. Treat those keys as reserved — a `traceparent` you set yourself
  is overwritten by the live trace context.

There is no headers overload on `PublishBatchAsync`; a batch carries only the injected
trace context (shared by every message in the batch).

## Batching

`PublishBatchAsync` publishes several messages to the **same topic** in one call:

```csharp
var events = shipments.Select(s => new ShipmentDispatched(s.Id, s.Carrier)).ToList();
await bus.PublishBatchAsync("shipments.dispatched", events, ct);
```

Semantics, in the order they happen:

- The **mode guard runs first** — a batch on a `ConsumeOnly` bus throws even when empty.
- An **empty list is a no-op**: no span, no serialization, no counter, immediate return.
- The serializer is resolved **once** for the `(bus, topic)` pair and every message is
  serialized up front, so a serialization failure surfaces before anything reaches the
  broker.
- The whole batch is handed to the transport's batch API where one exists —
  SQS uses `SendMessageBatch` (chunking to the broker's limit of 10 internally), RabbitMQ
  publishes the batch on one channel under a single confirm — and falls back to
  sequential sends otherwise. Either way you make one call.
- One **producer span** covers the batch, tagged `messaging.batch.message_count`; the
  `runax.messaging.published` counter increments by the batch size on success.

**Partial-failure caveat.** A batch is not a transaction. If the call throws, the
transport may already have delivered a prefix of the batch (or, for chunked APIs, some
chunks). Combined with at-least-once semantics the safe response is the same as for any
publish failure: retry the whole batch and let idempotent consumers absorb duplicates.
For an all-or-nothing guarantee, publish through the [outbox](#publishing-through-the-outbox)
inside your database transaction.

Since every envelope in a batch is serialized into memory before the handoff, chunk very
large sets yourself — see [the recipe](#chunking-large-batches).

## What a publish actually does

`Bus` runs a deliberately small pipeline:

```
bus.PublishAsync(topic, message[, headers])
        │
        ├─ 1. mode guard          ConsumeOnly → InvalidOperationException
        ├─ 2. producer span       "{topic} publish"; W3C trace context → headers
        ├─ 3. envelope serialize  body (per-bus / per-topic serializer) + __runax metadata
        └─ 4. sink                default: IMessagingTransport.PublishAsync(topic, envelopeJson)
                                  outbox:  IOutboxStore.AddAsync(...)   (sink swapped per bus)
                                        └─ broker (exchange / queue / stream / hub)
```

**The envelope, in one paragraph.** Your message is serialized at the top level and the
framework attaches a single reserved `__runax` key beside it carrying the contract name
and version (when the type has `[MessageContract]`) and the headers — including the
injected trace context. The transport only ever sees this one JSON string; it never
knows your message types. The full wire format, interop with foreign producers, and
custom body serializers are covered in [Serialization](serialization.md).

**Telemetry.** Every publish emits, under the `"Runax.Messaging"` activity source and
meter (see [Observability](observability.md)):

- a `Producer` span named `"{topic} publish"` tagged `messaging.system`,
  `messaging.destination.name`, `messaging.operation = publish`, and
  `messaging.runax.bus` — the bus name, which is what tells two buses on the same broker
  type apart on a dashboard. A failed handoff sets the span status to `Error` and
  rethrows.
- a `runax.messaging.published` counter increment (by 1, or by the batch size), tagged
  with the same system / destination / bus triple, recorded only after the sink accepts
  the handoff.

## Publishing through the outbox

Configuring a transactional outbox on a bus changes **nothing at the call site**. The
outbox package swaps step 4 of the pipeline — the bus's publish *sink* — so
`bus.PublishAsync` serializes exactly as before but writes the envelope to the registered
outbox store instead of the transport. With a durable store the write enlists in your
ambient database transaction, so "save the order + publish the event" commits or rolls
back as one; a background dispatcher delivers pending envelopes to the transport
afterwards.

```csharp
messaging.AddBus(bus =>
{
    bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
    bus.AddOutbox();
    bus.AddOutboxStore(new InMemoryOutboxStoreConfig());
});
```

The only observable differences: `PublishAsync` now completes when the **row is stored**
(delivery happens later), and delivery latency includes the dispatcher's polling cadence.
Everything else on this page — envelope, headers, telemetry, batching — applies
unchanged. See [Outbox](outbox.md).

## Recipes

### Fan-out to two buses

A bus wraps exactly one transport, so publishing one event to two brokers is two explicit
publishes — there is no implicit fan-out:

```csharp
public sealed class OrderEventPublisher(
    IBus bus,                                   // default bus: the order-processing broker
    [FromKeyedServices("audit")] IBus auditBus) // named bus: the audit broker
{
    public async ValueTask PublishPlacedAsync(OrderPlaced message, CancellationToken ct)
    {
        await bus.PublishAsync("orders.placed", message, ct);
        await auditBus.PublishAsync("orders.placed", message, ct);
    }
}
```

The two publishes are independent: if the second throws, the first has already been
delivered. When that matters, put an outbox on each bus and do both publishes inside one
database transaction — the two envelope rows then commit atomically.

### Contract-versioned messages

Stamp a version on the message type with `[MessageContract]`; it travels in the envelope
and consumers route on it (see [Message contracts](contracts.md) for design and rollout
guidance, and [Consuming](consuming.md) for the matching rules):

```csharp
[MessageContract(2)]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total, string Currency);

await bus.PublishAsync("orders.placed", new OrderPlaced(id, customerId, total, "USD"), ct);
```

With no `Name`, the contract's identity is the publish topic — messages are routed by
`(topic, version)`. Set `Name` when the contract's identity should be stable across
topics: `[MessageContract(2, Name = "orders.placed")]`. Types without the attribute are
unversioned and behave exactly as before.

### Publishing on a `ConsumeOnly` bus throws

A bus declared `bus.Mode = BusMode.ConsumeOnly` rejects every publish before anything is
serialized:

```csharp
var partnerFeed = buses.GetBus("partner-feed");   // a ConsumeOnly bus
await partnerFeed.PublishAsync("orders.placed", message, ct);
// InvalidOperationException: Bus 'partner-feed' is ConsumeOnly; publishing on it is not allowed.
```

This is a runtime guard on `IBus`; configuration-time violations (registering an outbox
on a `ConsumeOnly` bus, consumers on a `PublishOnly` bus) fail at startup instead. See
[Buses](buses.md).

### Chunking large batches

`PublishBatchAsync` serializes the whole list before the transport handoff. Transports
already chunk to *broker* limits (SQS's 10-per-request, for example), so chunk at the
call site only to bound memory and failure blast radius on very large sets:

```csharp
foreach (var chunk in events.Chunk(500))
    await bus.PublishBatchAsync("inventory.stock.recounted", chunk, ct);
```

Each chunk is its own producer span and its own at-least-once unit: on failure, retry
the failed chunk (not the whole set), accepting that a delivered prefix of it may be
duplicated.

## See also

- [Buses](buses.md) — registering buses, modes, the default bus
- [Consuming](consuming.md) — the other half: consumers, retries, dead-lettering
- [Serialization](serialization.md) — the `__runax` envelope and custom body serializers
- [Outbox](outbox.md) — atomic "save + publish" with a transactional outbox
- [Observability](observability.md) — spans, metrics, and health checks in depth
- [Writing a custom transport](writing-a-custom-transport.md) — the publish SPI
