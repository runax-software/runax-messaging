# Message contracts

A message contract is the shape two services agree on: the .NET type you publish, the JSON
it serializes to, and — when you opt in with `[MessageContract]` — a version number that
travels with every message. This page covers designing contract types, sharing them between
services, versioning and evolving them, and verifying version coverage with
`IMessageContractCatalog` before rolling out a producer change.

For how versions are *matched* at dispatch time, see
[Consuming & reliability](consuming.md); for the envelope that carries the version on the
wire, see [Serialization](serialization.md).

## Designing contract types

A contract type is a plain data carrier. Records fit naturally:

```csharp
[MessageContract(1)]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total);
```

Rules and recommendations:

- **The serialized form must be a JSON object.** The envelope serializer injects the
  reserved `__runax` metadata property into the message's own JSON object, so top-level
  arrays and primitives are rejected at publish time with an `InvalidOperationException`.
  Wrap collections in a carrier type (`record OrdersImported(IReadOnlyList<OrderLine> Lines)`).
- **Never declare a `__runax` property.** The key is framework-owned; a contract type that
  declares it is rejected at publish time.
- **Keep contracts immutable and behavior-free.** Records with init-only members make the
  intent explicit. A contract is data crossing a process boundary — logic belongs in the
  consumer, not the payload.
- **Prefer primitives and other contract types as members.** Whatever
  `System.Text.Json` can round-trip works (the body serializer is
  [replaceable](serialization.md), but the default is System.Text.Json with the options you
  configure per bus). Avoid framework types, entities with lazy-loading, or anything
  carrying identity beyond its data.
- **Name the type after the event, past tense** — `OrderPlaced`, `InvoicePaid`,
  `PartnerFeedReceived` — and keep the topic aligned (`order.placed`, `invoice.paid`).

## Sharing contracts between services

The contract type must deserialize identically on both sides, so producer and consumer
services usually share it. Two patterns work:

- **A contracts assembly** — a small class library holding only contract types, referenced
  by every participating service. It needs a reference to `Runax.Messaging.Abstractions`
  only when it uses `[MessageContract]` — and nothing else: the abstractions package is
  contracts-only by design, so a contracts assembly never drags in transports or the core
  implementation.
- **Duplicated types** — each service declares its own copy with the same shape. Routing
  never uses the CLR type name (only the topic, the version, and the JSON shape matter), so
  copies are wire-compatible as long as the properties match. This trades compile-time
  safety for looser coupling; it is the only option across language boundaries.

Application code that only publishes needs only `Runax.Messaging.Abstractions` — the
contracts assembly plus `IBus` is a complete publishing dependency set.

## `[MessageContract]` — declaring a version

Applying the attribute is optional. Types without it are **unversioned** and behave exactly
like plain payloads:

```csharp
[MessageContract(2)]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

What it does:

- **`Version`** travels in the envelope with every publish of this type
  (`__runax.contract_version`) and is matched against consumers at dispatch — messages are
  routed by **(topic, version)**.
- **`Name`** (optional) is a stable contract label carried as `__runax.contract_name` and
  surfaced on `MessageContext.ContractName` and `UnroutableMessage.ContractName`. It does
  **not** participate in routing — use it when the publish topic alone is not a durable
  identity (topics being renamed, several topics carrying the same logical contract) or for
  auditing and diagnostics:

```csharp
[MessageContract(2, Name = "orders.placed")]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

On the consuming side nothing extra is declared: a `MessageConsumer<OrderPlaced>` reads the
version from the `[MessageContract]` attribute on `OrderPlaced` itself. One message type =
one version; a consumer for V2 references the V2 type.

### Matching rules

| Consumer's message type | Wire message | Delivered? |
| --- | --- | --- |
| Unversioned (no attribute) | Any version, or unversioned | Yes — an unversioned consumer accepts everything on its topic |
| `[MessageContract(2)]` | `contract_version: 2` | Yes |
| `[MessageContract(2)]` | `contract_version: 1` | No |
| `[MessageContract(2)]` | Unversioned | No — a versioned consumer requires an exact version match |

A message no consumer on the bus accepts is **unroutable** and goes to the configured
strategy — dead-letter by default. See
[unroutable messages](consuming.md#unroutable-messages).

## Evolving a contract

Because the body is JSON, not every change needs a new version.

**Additive changes — no version bump needed:**

- Adding a property with a sensible default (`string? Note = null`,
  `decimal Discount = 0`). Old consumers ignore the extra JSON property; new consumers
  see the default when an old producer omits it.
- Widening documentation, renaming the CLR *type* (the type name is not on the wire), or
  moving the type between namespaces/assemblies.

**Breaking changes — bump the version:**

- Removing or renaming a *property* (the JSON name is the wire format).
- Changing a property's type or meaning (`Total` switching from net to gross).
- Making a previously-optional property required.

When you bump, keep both versions live for the transition:

```csharp
[MessageContract(1)]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total);

[MessageContract(2, Name = "orders.placed")]
public sealed record OrderPlacedV2(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

Both types publish to the same topic; consumers for V1 and V2 run side by side on the same
bus, each receiving only its own version. The full consumer-side example is in
[Evolving a contract side by side](consuming.md#evolving-a-contract-side-by-side).

Retire V1 by removing its producer first, draining, then removing the V1 consumer —
production order is the subject of the next section.

## Rolling out a new version safely

The failure mode to avoid: a producer starts emitting V2 while some consumer deployments
only understand V1. Those messages are unroutable — dead-lettered by default. The safe
order is therefore **consumers first, producer last**:

1. Ship the V2 consumer (alongside the V1 consumer) to every consuming service.
2. Verify coverage (below).
3. Ship the producer change that emits V2.
4. Once no V1 traffic remains (watch the `runax.messaging.consumed` metric by topic, or
   your broker's queue metrics), delete the V1 consumer and type.

### Verifying coverage with `IMessageContractCatalog`

`IMessageContractCatalog` (registered automatically by `AddRunaxMessaging`) introspects
which `(topic, version)` pairs this application's registered consumers handle — across all
of its buses:

```csharp
public interface IMessageContractCatalog
{
    IReadOnlyCollection<HandledContract> Handled { get; }   // record HandledContract(string Topic, int? Version)
    bool Accepts(string topic, int version);
}
```

`Accepts(topic, version)` is true when a consumer for exactly that version — or an
unversioned accept-all consumer — is registered for the topic. Use it as a startup gate so
a deployment that forgot a consumer fails loudly instead of dead-lettering traffic:

```csharp
var app = builder.Build();

// Fail fast if this deployment cannot handle the contract versions we expect to receive.
var catalog = app.Services.GetRequiredService<IMessageContractCatalog>();
foreach (var (topic, version) in new[] { ("order.placed", 2), ("invoice.paid", 1) })
{
    if (!catalog.Accepts(topic, version))
        throw new InvalidOperationException(
            $"No consumer accepts '{topic}' v{version}. Deploy the matching consumer before this producer version.");
}

app.Run();
```

The same check works in a deployment pipeline: a smoke test that builds the host and
asserts `Accepts(...)` for every contract the surrounding system emits, before traffic is
switched over. `Handled` supports the inverse audit — dumping what an application consumes:

```csharp
foreach (var contract in catalog.Handled)
    logger.LogInformation("Consumes {Topic} v{Version}", contract.Topic, contract.Version?.ToString() ?? "any");
```

Note the catalog is application-wide, not per bus: it answers "would *some* consumer in
this process accept this message", which is the question a rollout gate needs. Messages
still only reach consumers on the bus they arrive on.

### The safety net

Even with a gate, an unexpected version can arrive (an old producer restarted from a stale
image, a partner shipping early). That is what the unroutable pipeline is for: the default
dead-letter strategy preserves the message — with `x-runax-dlq-*` headers recording the
unaccepted version — so it can be replayed once the consumer catches up. Configure the
strategy per bus with `OnUnroutableMessage`; see
[unroutable messages](consuming.md#unroutable-messages).

## See also

- [Consuming & reliability](consuming.md) — version matching in the dispatch pipeline,
  side-by-side consumers, unroutable handling
- [Publishing](publishing.md) — how the version travels with a publish
- [Serialization](serialization.md) — the `__runax` envelope, body serializers, AOT
- [Buses](buses.md) — the bus model contracts flow through
