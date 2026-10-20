# Spec: JSON Schema contracts

| | |
| --- | --- |
| **Status** | In progress. Phase 1 core and the JSON Schema package are implemented; see [Implementation status](#implementation-status). |
| **Date** | 2026-10-19, revised 2026-10-20 |
| **Replaces** | Integer contract versions (`[MessageContract(version)]`) |
| **Depends on** | Native broker headers ([#71](https://github.com/runax-software/runax-messaging/pull/71)) |
| **Roadmap** | "JSON Schema contracts" in [ROADMAP.md](../../ROADMAP.md) |

## Summary

- **Identity.** A contract is identified on the wire by its JSON Schema's `$id`, an absolute
  URI with one value per breaking line (`…/order-placed/v2`). It travels in a new envelope key,
  `__runax.contract_schema`, and a new native header, `runax-contract-schema`. These sit next to
  the existing name and version slots, so transports don't change.
- **Matching.** Consumers match the `$id` exactly. Compatibility is proven in CI, not worked
  out at runtime. A consumer type can list extra `$id`s it can read, and CI checks that claim.
- **Identity is optional on the wire.** A message that carries neither a `$id` nor an
  integer is routed by validating its body against the schemas its topic's consumers
  declare. Lines that would accept any JSON object are left out. If several lines that reach
  different consumers accept it, no schema consumer gets it, rather than guessing, while
  accept-all consumers still do. A bus can require identity instead.
- **Source of truth.** A committed schema file per `$id`. .NET-owned contracts export it from
  the C# type with `JsonSchemaExporter`, which is AOT-clean (verified below), and CI fails if
  the file drifts from the type. Go, Java and Node teams either author the file by hand or
  export it from their own types.
- **Gate.** The `runax-messaging contracts` commands of the `Runax.Messaging.Cli` tool, shipped as a native binary so non-.NET pipelines don't
  need the .NET SDK. It checks that every revision of a `$id` is fully compatible with all
  earlier ones, and that each service's types conform to the committed file.
- **Validation.** An optional `ISerializer` decorator validates on publish and/or consume.
  It's off by default and can either report or enforce.
- **Broker-native validation.** When validation is on, the library validates on every bus by
  default. A messaging-level default sets this, and each bus copies and can override it. A
  transport whose broker ecosystem validates natively advertises that as a capability. On
  such a bus, library validation can be turned off so the broker validates alone. Kafka with
  Confluent Schema Registry comes first, as an add-on. Confluent's broker-side check only
  confirms that a registered schema ID is attached, so the add-on also validates payloads
  client-side. It carries the registry ID in a header, never as a payload prefix, so the body
  stays plain JSON.
- **Migration.** Producers write both the integer and the `$id` while they coexist. 2.x
  services keep routing by the integer. The integer lives in each schema file
  (`x-runax-legacy-version`), so a service that embeds its documents keeps writing and
  accepting it after the attribute drops it. The integer constructor is deprecated in 3.0.

## Implementation status

Built so far, following the recommendations in §11 unless noted:

| Part | Status |
| --- | --- |
| §3 Wire identity: `Schema`/`Reads`, `contract_schema`, `runax-contract-schema`, merge, normalization | Done |
| §4 Matching: the rule, legacy versions from lines, routing by validation, selectivity, ambiguity (accept-all consumers unaffected), `RequireContractIdentity`, `ContractSchemaMismatch` | Done |
| §4 Unroutable reasons, span attributes, metrics, catalog additions | Done |
| §5 Embedded schema provider (`AddJsonSchemaContracts`, `AddEmbeddedSchemas`, `AddSchema`) | Done |
| §6 Profile evaluator (in-house, open question 6) with formats asserted by System.Text.Json's rules | Done |
| §6 Runtime validation (`bus.ValidateContracts`), the `DecorateSerializer` hook, `ContractValidationException` | Done |
| §4 Startup overlap, non-selective and missing-document warnings | Done |
| §6 `runax-messaging contracts lint` and `check` (`Runax.Messaging.Cli`, a .NET tool) | Done |
| §6 `contracts conform`, native binaries and a container image | Not started |
| §5 Export (`contracts export`, `ContractSerialization`) and `VerifyContractSchemas` | Not started |
| §7 Messaging-level validation default, `IContractValidatingTransport`, the Kafka Schema Registry add-on | Not started |
| §8 Phase 3 (parameterless constructor, deprecations) | Not started, by design: 3.0 |

Deviations from the text below:

- The tool is `Runax.Messaging.Cli` with the command `runax-messaging`, and contract commands are grouped
  under `contracts` (`runax-messaging contracts lint`), so other areas can be added later. It's built on
  System.CommandLine. Text output and GitHub Actions annotations (`--format github`) replace SARIF for now.

- `DecorateSerializer` takes `Func<IServiceProvider, ISerializer, ISerializer>`, so decorators can
  resolve services such as loggers.
- A message routed by validation is validated again when consume validation is on. Skipping the
  second check (§4) needs the serializer to see the message context, which it doesn't today. It
  only costs time.
- `IContractSchemaMatcher` is internal to core and implemented by the package through
  `InternalsVisibleTo`, until the design settles.

## 1. Problem

`[MessageContract(2)]` is a label, not a check. Two services that both say "version 2" can
disagree on the shape, and nothing notices. At best deserialization fails. At worst a renamed
property silently arrives as its default. In practice:

- **Duplicated types drift.** [contracts.md](../contracts.md) allows each service to declare
  its own copy of a type, and for Go, Java and Node producers that's the only option. Whether
  the copies stay wire-compatible is only a convention.
- **Version bumps aren't coordinated.** Two teams can both move to `2` with different shapes.
- **The evolution rules are review-only.** The additive-vs-breaking list in contracts.md isn't
  enforced anywhere.
- **Types sharing a topic can't be told apart.** Routing is `(topic, version)`, so
  `OrderPlaced` v1 and `OrderCancelled` v1 on one topic are both delivered to both consumers.

### Goals

- A language-neutral contract identity that tooling in every language can resolve.
- A machine-checked compatibility gate, both between revisions of a contract and between a
  service's types and the contract.
- Wire compatibility with 2.x (and 1.x) services through a coexistence period.
- Runtime pieces that are trim- and native-AOT-safe, like every `src/` project.
- No transport changes for identity and routing.
- **Reuse a broker's own schema validation where it exists, with library validation layered
  on top** (§7). When validation is turned on, the library validates by default. A bus whose
  broker ecosystem validates natively (Kafka with Confluent Schema Registry first) can turn
  library validation off and leave validation to the broker alone. Core models this as a
  per-transport capability, not as anything Kafka-specific.

### Non-goals

- A dependency on a schema registry in core, or fetching schemas over the network from core
  or the library validator. A transport that integrates its broker's registry (§7) does
  that inside its own package.
- Avro, Protobuf, or validating non-JSON bodies.
- Registry integrations in core. Confluent Schema Registry, Azure Schema Registry and AWS
  Glue are tied to one broker ecosystem, so they plug in through a transport capability
  (§7) and live in transport packages. Core stays transport-agnostic.

## 2. Today's behavior (baseline)

Assuming [#71](https://github.com/runax-software/runax-messaging/pull/71) is merged:

- `[MessageContract(version, Name = …)]` on a type sets an `int` version and an optional
  non-routing name. Publishing writes `__runax.contract_version` / `contract_name` and the
  native headers `runax-contract-version` / `runax-contract-name` (`MessageHeaders`).
- On receive, envelope and native values are merged key by key per `HeaderPrecedence`.
  `MessageConsumerHostedService.MatchConsumers` accepts a consumer when it's unversioned or
  its version equals the wire version. If nothing matches, the message goes to the bus's
  `IUnroutableMessageHandler` (dead-letter by default).
- `IMessageContractCatalog` lists the `(topic, version)` pairs consumers handle, and
  `Accepts(topic, version)` answers the rollout question.

Some existing behavior constrains the design. Each point below was checked against the code
on the #71 branch:

| Behavior | Consequence |
| --- | --- |
| The envelope reader casts `contract_version` with `(int?)JsonNode`. A string or non-integral number **throws**, so the message is treated as a malformed envelope and dead-lettered. | The new identifier **cannot** reuse the `contract_version` key. A string there would make every 2.x consumer dead-letter the message. |
| The envelope reader ignores `__runax` keys it doesn't know. | A new key like `contract_schema` is invisible to 2.x and 1.x readers. |
| `runax-contract-version` values that aren't integers are ignored. Every other native header, including any new `runax-*` one, becomes an ordinary entry in `MessageContext.Headers` and is kept on dead-letter copies. | A new native header is harmless to 2.x. It shows up as a plain header and survives a dead-letter round trip. |
| `MessageHeaders.InPriorityOrder` writes contract metadata first, so it fits SQS/SNS's 10-attribute cap. | The new header joins the priority list. That's a one-line change in Abstractions, not in transports. |

## 3. Wire identity

### Options

| | Schema `$id` URI | Content fingerprint | Name + semver |
| --- | --- | --- | --- |
| Example | `https://contracts.acme.dev/orders/order-placed/v2` | `sha256:9f2c…` of the canonical schema | `orders.placed@2.1.0` |
| Proves the shapes match | No, it's a name. CI proves it. | Only that two **documents** are byte-equal | No, it's a claim. CI proves it. |
| Stable across additive changes | Yes, by convention, and CI enforces it | No. Every change is a new identity. | Major yes; minor and patch change |
| Same for the same shape across languages | Yes, it's assigned | **No.** The STJ exporter, Go's `invopop/jsonschema` and Java's victools generator emit equivalent but different documents. | Yes, it's assigned |
| Runtime needs | Nothing | A registry to decide whether an unknown fingerprint is compatible | A range rule (`^2`) |
| Resolvable by standard tools | Yes. Every JSON Schema validator understands `$id` / `$ref`. | No | No, it needs a custom convention |
| Readable in a dead-letter queue | Yes | No | Yes |
| Interop | Maps 1:1 to the CloudEvents `dataschema` attribute | — | — |

A fingerprint doesn't work as an identity. It isn't reproducible across languages, and
because every additive change produces a new value, consumers would need a schema registry
at runtime. Name + semver reads well, but once CI guarantees that everything within a major
is compatible, minor and patch carry no routing information. It also isn't a concept any
JSON Schema tool understands. And it can't go in `contract_version` anyway (see §2).

### Recommendation: `$id`, one per breaking line

- **Value.** The schema document's `$id`, an absolute URI. Convention:
  `<base>/<domain>/<contract>/v<major>`, for example
  `https://contracts.acme.dev/orders/order-placed/v2`. It doesn't have to be dereferenceable.
  The library never fetches it.
- **Lifecycle.** Additive revisions keep the `$id`. A breaking change mints a new one (`…/v3`).
  CI enforces both directions (§6). This is the same rule CloudEvents gives for
  `dataschema`: incompatible schema changes should get a different URI.
- **Comparison.** Ordinal string equality, after dropping an empty trailing fragment (`#`).
  No case folding or URI normalization. The linter rejects non-canonical forms instead.
- **Size.** At most 256 bytes, checked by the linter. That keeps it well inside the Pub/Sub
  1 KB value limit and SQS attribute budgets.

### Where it travels

| Metadata | In the envelope | As a native header | Status |
| --- | --- | --- | --- |
| Contract schema (`$id`) | `__runax.contract_schema` | `runax-contract-schema` | **New**, used for routing |
| Contract name | `__runax.contract_name` | `runax-contract-name` | Unchanged, label only |
| Contract version (`int`) | `__runax.contract_version` | `runax-contract-version` | Legacy, written during coexistence (§8) |

```json
{
  "orderId": "8a6f…",
  "customerId": "c-42",
  "total": 18.5,
  "__runax": {
    "contract_schema": "https://contracts.acme.dev/orders/order-placed/v2",
    "contract_name": "orders.placed",
    "contract_version": 2,
    "headers": { "traceparent": "00-…" }
  }
}
```

The identifier stays in the same places #71 set up: the `__runax` object and the `runax-`
native header namespace. It gets its own key only because the integer key can't hold a
string without breaking 2.x readers. `MessageHeaders.ContractSchema` becomes a public
constant, and the priority order becomes schema, name, version, `traceparent`, `tracestate`,
then the rest.

The header is merged with the envelope copy like the existing contract headers: key by key,
with `HeaderPrecedence` deciding conflicts, and it isn't repeated in `MessageContext.Headers`.
`MessageContext` gains `string? ContractSchema`.

A producer in another language can set the header, or send nothing at all. **Identity on
the wire is optional.** A message with no `$id` and no integer is routed by validating its
body against the schemas its topic's consumers declare (§4).

```go
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value:          body,
    // Optional. Without these headers, a Runax consumer routes the message by validating the
    // body against the schemas it declares (§4). Setting them is cheaper, and unambiguous.
    Headers: []kafka.Header{
        {Key: "runax-contract-schema", Value: []byte("https://contracts.acme.dev/orders/order-placed/v2")},
        {Key: "runax-contract-version", Value: []byte("2")}, // only while 2.x consumers remain (§8)
    },
}, nil)
```

## 4. Matching

### Exact `$id`, with compatibility proven ahead of time

Runtime compatibility matching ("accept if the producer's schema is compatible with mine")
is rejected. The consumer would need the producer's schema at runtime, which means either a
network registry or embedding every revision. Routing would then depend on a remote service.
And the compatibility decision would happen after the message is already in flight, which is
exactly what the gate is supposed to prevent.

Within a `$id`, CI guarantees that every revision is compatible with every other (§6). That
makes exact `$id` matching a compatibility-based match, decided at build time.

### Extra readable `$id`s: `Reads`

Sometimes one type can read two breaking lines. For example, v3 dropped `legacyCode`, which
v2 required. That's breaking, because v2 readers require the field. But a v3 reader simply
ignores it in a v2 message, so the v3 type can read both lines and can say so:

```csharp
[MessageContract(3, Schema = OrderContracts.OrderPlacedV3, Reads = [OrderContracts.OrderPlacedV2])]
public sealed record OrderPlacedV3(Guid OrderId, string CustomerId, decimal Total);
```

Adding a field the type defaults (`string Currency = "USD"`) is **not** such a case. The
exporter marks a defaulted parameter optional, so that change is additive and stays within
the same `$id` (§6).

A producer of this type always writes `Schema`. `Reads` only widens which messages a
consumer of the type accepts. CI's conformance check (§6) verifies that every message valid
under each listed `$id` is readable by the type, so `Reads` is a claim that gets checked,
never something the runtime infers.

### The rule

For each consumer, let *S* be its type's `Schema` plus `Reads` (empty without `Schema`).
Let *V* be the legacy integers it accepts: the attribute's integer, plus the
`x-runax-legacy-version` of every line in *S* whose document is embedded (§5). CI's
conformance check keeps the attribute's integer equal to its `Schema` line's, so the two
sources never disagree. *V* is what lets a consumer that declares only `Schema` keep
accepting int-only messages from 2.x and foreign producers, and a `Reads` v2 consumer accept
an int-only v2 message. *Inferred* is the set of `$id`s the body validated against when the
message carried no identity, after the ambiguity rule (next section). It's empty otherwise,
and empty for an ambiguous message. The dispatcher accepts the consumer when:

```csharp
static bool Accepts(IReadOnlySet<string> s, IReadOnlySet<int> v, string? wireSchema, int? wireVersion,
    IReadOnlySet<string> inferred)
{
    if (s.Count == 0 && v.Count == 0)
        return true;                              // unversioned: accepts everything, as today

    if (wireSchema is not null && s.Count > 0)
        return s.Contains(wireSchema);            // both sides have schema ids: they decide

    if (wireVersion is not null)
        return v.Contains(wireVersion.Value);     // otherwise fall back to the integer

    return wireSchema is null && inferred.Overlaps(s);   // no identity at all: route by validation
}
```

The precedence is: a wire `$id` decides when present, from the envelope or a header. Without
one, the integer decides. Only a message that carries neither is routed by validation.

**Adoption safety valve.** During phase 2 (§8), a consumer that just added `Schema` stops
accepting a message whose `$id` doesn't match, even though its integer still does. That's
the point, but a producer that ships a mistyped `$id` would then dead-letter traffic that
used to flow. `bus.ContractSchemaMismatch = SchemaMismatch.Report` lets such a message
through by its integer instead, while logging a warning and recording
`runax.messaging.contract.schema_mismatch`. It only applies when both sides also carry
integers that match. The default is `Reject` (open question 29).

| Consumer declares | Wire carries | Result |
| --- | --- | --- |
| Nothing (unversioned) | Anything | Delivered (unchanged) |
| `Schema` (+ int) | Same `$id` | Delivered |
| `Schema` (+ int) | A `$id` in `Reads` | Delivered |
| `Schema` + int 2 | Different `$id`, int 2 | **Not delivered.** A mismatched `$id` is exactly the drift this feature exists to catch, so there's no fallback to the integer here, unless the bus opts into the adoption safety valve below. |
| `Schema` + int 2 | No `$id`, int 2 (2.x or foreign producer) | Delivered by the integer |
| `Schema` only (3.0) | No `$id`, int 2 | Delivered if 2 is the `x-runax-legacy-version` of one of its lines, which needs the documents embedded (§5). Otherwise not delivered: the integer is an identity claim, so it isn't validated (open question 27). |
| `Schema` (+ int) | **No `$id`, no int** | **Delivered if the body validates against one of the consumer's lines, and the match isn't ambiguous** (next section) |
| int 2 only (legacy) | `$id` + int 2 (dual-writing producer) | Delivered by the integer |
| int 2 only (legacy) | `$id` only | Not delivered |
| int 2 only (legacy) | No `$id`, no int | Not delivered: there's no schema to validate against (unchanged) |

Because `$id`s are globally unique, several message types can share a topic and still route
correctly, which today's integer can't do.

### Messages without identity: routing by validation

A producer doesn't have to say which contract it sent. A message whose envelope and headers
carry neither a `$id` nor an integer is routed by validating it against what the consumers
declare. That covers foreign producers that send plain JSON, and Runax producers of types
without `[MessageContract]`.

1. **Candidates.** The candidate lines are the `$id`s in the *S* sets of the topic's
   consumers on this bus. Each candidate's canonical document comes from the embedded schema
   provider (§5).
2. **Validate once per line.** Core parses the body once, with `__runax` already removed,
   and evaluates it against each candidate. *Inferred* is the set of lines that accept it.
3. **Resolve ambiguity.** Readers ignore unknown properties (§6), so a v2 body often also
   validates against the v1 schema. *Inferred* is ambiguous when two of its lines would
   reach different consumers, meaning some consumer declares one line but not the other. An
   ambiguous message goes to **none of the schema-declaring consumers**: *Inferred* becomes
   empty, nothing is guessed, and nothing is delivered twice. Unversioned (accept-all)
   consumers still receive it, exactly as they do today, so adding a schema consumer to a
   topic can never cut off an existing accept-all one. As with any message, it's unroutable
   only when no consumer received it at all. Either way it's logged as a warning and
   recorded as `runax.messaging.contract.ambiguous` (open question 25).
   - Lines whose consumers are the same don't conflict. That happens with a type that
     declares v3 with `Reads` v2, or when every consumer reads both lines.
   - With one line per topic, which is the common case, ambiguity never arises.
4. **No match.** If no candidate accepts the body and no unversioned consumer exists, the
   message is unroutable.

More rules:

- **Only selective lines take part.** Readers ignore unknown properties, so a line with no
  required property and no `const` discriminator accepts every JSON object. Routing by it
  would deliver any foreign traffic on the topic as long as it's an object. Such lines are
  left out of the candidates: their consumers need identity on the wire. `lint` and the
  startup check flag them, and adding a required discriminator or any required property
  makes the line selective (open question 28). A selective line can still be broad, which
  is the honest limit of routing by shape: a consumer receives whatever its schema accepts.
- **Routing validation is separate from `ValidateContracts`.** It runs whenever a message
  has no identity and the topic has schema-declaring consumers, even when the bus's
  `Report`/`Enforce` validation is off, or when library validation is off in favor of the
  broker (§7). Routing needs an answer in-process. The cost only applies to unidentified
  messages, and a small number of lines per topic.
- **The result doubles as consume-side validation.** A message delivered this way has
  already passed its consumer's line, so the §6 consume check is skipped for it.
- **Inferred identity stays separate from claimed identity.** `MessageContext.ContractSchema`
  keeps reporting only what the wire carried, which here is nothing. The lines the body
  matched go on a new `MessageContext.InferredContractSchemas` and the process span
  (`messaging.runax.contract.inferred`), so handlers and logs can tell a claim from a guess.
- **Dead-letter copies don't get the inferred `$id` stamped on them.** A replay goes through
  inference again, against whatever consumers exist then.
- **Strict buses can opt out** with `bus.RequireContractIdentity = true`. Unidentified
  messages then never reach schema consumers, as today, and the default is open question 24.
- **The machinery lives in the JSON Schema package.** Core can't validate on its own: it has
  no JSON Schema evaluator, by design. `Runax.Messaging.Contracts.JsonSchema` registers a
  matcher that core calls only for unidentified messages. Without the package, unidentified
  messages behave as they do today.

**Spotting overlap early.** Ambiguity is a property of the schemas a topic's consumers
declare, so it can be found before any message arrives:

- **At startup**, the catalog knows every consumer and line per topic on a bus. It warns when
  two lines reach different consumers and could both accept the same message.
- **In CI**, `runax-messaging contracts` runs the same check over consumer manifests (open question 10).

General schema overlap is hard to decide, so the check is conservative. Two lines count as
disjoint only when they share a required property whose `const`, `enum` or `type` values
don't intersect, such as a `type` discriminator. Every other pair gets a warning, and adding
a discriminator property clears it (open question 26).

### Unroutable messages

- `UnroutableMessage` gains `string? ContractSchema`. `ContractVersion` stays, and is marked
  obsolete in 3.0.
- The unroutable reason names the `$id` when there is one, for example "No registered
  consumer accepts contract schema 'https://…/order-placed/v3' on topic 'order.placed'." It
  appears in `x-runax-dlq-reason` and in the warning log.
- For an unidentified message, the reason lists the first failing JSON pointers per
  candidate line, for example "No consumer's schema accepts this unidentified message:
  …/order-placed/v2 at /currency (required)". An ambiguous message's reason names the
  conflicting lines instead.
  - The header copy is truncated to 512 bytes so it fits every broker's limits (Pub/Sub
    allows 1 KB values). The warning log carries the full list.
  - `UnroutableMessage` gains `UnroutableReason` (`NoMatchingContract`, `NoMatchingSchema`,
    `AmbiguousSchema`) so custom handlers can tell the cases apart.
- Dead-letter copies keep `runax-contract-schema` natively and `contract_schema` in the
  envelope. `NativeHeaders.Build` / `FromEnvelope` gain the field, so a replayed message
  routes exactly like the original, and so does an outbox entry.
- The process span gets a `messaging.runax.contract.schema` attribute.
- **Unroutable is not the same as invalid.** Unroutable means no consumer handles this `$id`,
  and the unroutable strategy decides what happens. Invalid means a consumer handles the
  `$id` but the body fails validation. That's poison and gets dead-lettered (§6).

### `IMessageContractCatalog`

```csharp
public sealed record HandledContract(string Topic, int? Version)
{
    public string? Schema { get; init; }   // non-positional: `var (topic, version) = …` keeps compiling
}

public interface IMessageContractCatalog
{
    IReadOnlyCollection<HandledContract> Handled { get; }   // one entry per (topic, accepted $id); Reads included
    bool Accepts(string topic, int version);                // unchanged; [Obsolete] in 3.0
    bool Accepts(string topic, string schema);              // new
    bool Accepts(string topic, string? schema, int? version); // new: exactly the dispatch rule above
}
```

The rollout gate in contracts.md becomes:

```csharp
if (!catalog.Accepts("order.placed", OrderContracts.OrderPlacedV3))
    throw new InvalidOperationException("Deploy the …/order-placed/v3 consumer before this producer.");
```

A later, optional step (open question 10): `runax-messaging contracts manifest` serializes `Handled`
into a language-neutral consumer manifest. A fleet-level deploy gate can then check that
every `$id` a producer emits is accepted by every consuming service, including Go, Java and
Node consumers that write their manifests by hand.

## 5. Where schemas come from

### Options

- **A. Generated from C# types.** C# is the source of truth and other languages consume the
  export. This is the least effort for .NET teams, but a Go team that owns a producer has to
  edit C# to change its own contract. The export also inherits System.Text.Json's quirks
  (below) and can shift between .NET versions.
- **B. Hand-authored files.** The file is the source of truth and every language generates
  code from it or is checked against it. Fully language-neutral, but .NET developers write
  JSON Schema by hand, and C# types drift unless they're checked or generated.
- **C. Recommended hybrid: the committed file is the source of truth, and the contract's
  owner chooses how to produce it.** Every `$id` has one owner, usually the producer. The
  canonical document is committed (where is open question 2) and is the only thing the gate,
  other languages, and runtime validation read.

```
schemas/orders/order-placed/v2.schema.json   # $id …/orders/order-placed/v2; the path must match the $id (linted)
```

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://contracts.acme.dev/orders/order-placed/v2",
  "title": "OrderPlaced",
  "x-runax-legacy-version": 2,
  "type": "object",
  "properties": {
    "orderId": { "type": "string", "format": "uuid" },
    "customerId": { "type": "string" },
    "total": { "type": "number" },
    "currency": { "type": "string" },
    "note": { "type": ["string", "null"], "default": null }
  },
  "required": ["orderId", "customerId", "total", "currency"]
}
```

`x-runax-legacy-version` keeps the coexistence-period integer next to the `$id` in a
language-neutral place. It makes the integer a property of the **line**, not of the C# type:

- Foreign producers read it to set `runax-contract-version`.
- .NET conformance checks that the attribute's integer matches it.
- When the documents are embedded (below), a consumer accepts the integers of all its lines
  (§4), and a producer whose attribute omits the integer still writes its line's. That's
  what makes dropping the integer from the attribute in 3.0 safe (§8).
- The linter checks that a `$id` never changes its integer and that two `$id`s of one
  contract never share one (§8).

### .NET-owned contracts: export and commit

- `runax-messaging contracts export` loads the contracts assembly, finds every
  `[MessageContract(Schema = …)]` type, exports it with `JsonSchemaExporter`, stamps
  `$schema`, `$id`, `title` and `x-runax-legacy-version` through `TransformSchemaNode`, and
  writes the file whose path matches the `$id`.
- CI runs the export and fails on any diff, like a public-API snapshot test. A deliberate
  change is a re-export plus commit, and that diff is what reviewers and the evolution check
  see.
- **The export uses the wire options, not defaults.** The schema has to describe what's
  actually on the wire, so the contracts assembly declares its serialization once, as a
  source-generated context. AOT apps need that context anyway:

  ```csharp
  [assembly: ContractSerialization(typeof(OrderContractsJsonContext))]

  [JsonSourceGenerationOptions(
      PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
      UseStringEnumConverter = true,
      NumberHandling = JsonNumberHandling.Strict,
      RespectNullableAnnotations = true,
      RespectRequiredConstructorParameters = true)]
  [JsonSerializable(typeof(OrderPlaced))]
  public sealed partial class OrderContractsJsonContext : JsonSerializerContext;
  ```

  Buses that use these contracts register the same context
  (`o.TypeInfoResolver = OrderContractsJsonContext.Default`). If a bus overrides the naming
  policy, the startup self-check below catches it.

**What `JsonSchemaExporter` does.** Verified with the .NET 10.0.103 SDK by a probe app
that builds with `IsAotCompatible`, `PublishAot` and warnings as errors:

| Finding | Design consequence |
| --- | --- |
| The `JsonTypeInfo` overload, and the `JsonSerializerOptions` overload with a source-generated resolver, raise **no trim or AOT warnings**. The native AOT binary exports schemas correctly. | Export can run inside AOT apps, which makes a startup self-check possible (below). Without a resolver the options overload needs reflection, which AOT disables. |
| It emits no `$schema` or `$id`. A root-level `TransformSchemaNode` adds them. | The tool stamps them. |
| Objects are open (no `additionalProperties`) unless `JsonUnmappedMemberHandling.Disallow` is set, in which case it emits `additionalProperties: false`. | The contract profile recommends open objects (§6). |
| Enums export as `integer` unless a string enum converter is set, and then as `enum: [names]`. | Integer enums are a cross-language hazard, so the export uses `UseStringEnumConverter`. |
| `JsonNumberHandling.AllowReadingFromString` (on by default in `JsonSerializerDefaults.Web`) widens numbers to `["string","number"]` with a `pattern`. That describes what the reader tolerates, not what producers write. | The export uses `Strict`. |
| `string?` becomes `["string","null"]`. Constructor parameters without defaults are listed in `required`, and ones with defaults are optional and carry `default`. | These match the evolution rules in contracts.md. |
| Polymorphic types export as `anyOf` branches with a `const` discriminator. | Allowed in the profile, and diffed per branch. |
| Recursive types use JSON-pointer `$ref`s into the document (`#/properties/next`), not `$defs`. | The checker compares structure after resolving references, never `$ref` strings. |
| XML doc comments aren't exported. | Descriptions need a `TransformSchemaNode` hook, for example one that reads `[Description]`. |

**Custom body serializers.** The exporter only knows System.Text.Json. A contract
serialized by Json.NET or any other `ISerializer` has to have a hand-authored file, and only
runtime validation can catch drift for it.

### Non-.NET producers and consumers

Every route is checked against the committed file. The spec doesn't mandate a generator.

1. **Generate types from the schema (recommended).** For example quicktype or
   `go-jsonschema` for Go, jsonschema2pojo for Java, `json-schema-to-typescript` for Node.
   Types can't drift because they're regenerated from the file.
2. **Keep hand-written types**, export their schema with the language's own generator (for
   example `invopop/jsonschema`, victools `jsonschema-generator`, or Zod's JSON Schema
   output), and run `runax-messaging contracts conform` against the canonical file.

.NET services that consume contracts owned by other teams use the same `conform` check on
their C# copies. Generating C# from the file is possible later, for example with a Roslyn
source generator over `AdditionalFiles`, which is AOT-friendly because it runs at compile
time. That's open question 3.

### Schemas at runtime

Validation, the self-check, and routing messages that carry no identity (§4) need the
documents in-process. The
`Runax.Messaging.Contracts.JsonSchema` package embeds the committed files as
`EmbeddedResource`s through an MSBuild item glob, and an `IContractSchemaProvider` looks
them up by `$id`. That means no network and no file system, and manifest resource streams
are AOT-safe.

An optional **startup self-check**, `bus.VerifyContractSchemas()`, exports each consumed
`Schema` type through the bus's actual `JsonTypeInfo`, which is AOT-safe per the probe. It
then runs the conformance check against the embedded canonical document and logs or throws
on a mismatch. This catches the one drift CI can't see: a bus that configures different JSON
options than the contracts context. Topics using a custom `ISerializer` are skipped with a
warning.

## 6. The CI compatibility gate and runtime validation

### The `runax-messaging contracts` commands

The tool ships as a .NET tool and as a native AOT single-file binary (linux-x64,
linux-arm64, osx-arm64, win-x64) plus a container image, so Go, Java and Node pipelines
don't need the .NET SDK. Apart from `export`, it only reads schema files.

| Command | Checks |
| --- | --- |
| `lint <dir>` | Profile rules: absolute `$id` ≤ 256 bytes, file path matches `$id`, `$schema` is 2020-12, only supported keywords, no closed root object (§7), `x-runax-legacy-version` consistent |
| `check <dir> --history git` | **Evolution:** each changed file against every earlier revision of the same `$id`, read from the file's git history (needs `fetch-depth: 0`). `--baseline <dir>` is an alternative. |
| `conform --canonical <file> --writer <schema>` / `--reader <schema>` | **Conformance:** a service's own types against the canonical file |
| `export --assembly <dll>` | .NET only: writes schema files from `[MessageContract(Schema = …)]` types |

Output is human-readable text plus SARIF, for PR annotations. A nonzero exit fails the job.

```yaml
- uses: actions/checkout@v5
  with: { fetch-depth: 0 }          # `check` compares against every earlier revision
- run: dotnet tool install --global Runax.Messaging.Cli
- run: runax-messaging contracts lint schemas/
- run: runax-messaging contracts check schemas/ --history git
```

### The contract profile

Deciding inclusion between arbitrary JSON Schemas isn't tractable, so the checker supports
the subset that generators emit and hand-authors need: `type` (including `null` unions),
`properties`, `required`, `items`, `enum`, `const`, `format`, `default`, `anyOf`/`oneOf`
discriminated by `const`, local `$ref`/`$defs`, `additionalProperties`, the numeric and
string constraints (`minimum`, `maxLength`, `pattern`, …), and annotations (`title`,
`description`, `examples`, `deprecated`, `$comment`, `x-*`).

Other keywords make `lint` fail: `if`/`then`/`else`, `not`, `patternProperties`,
`dependentSchemas`, `unevaluated*`, and remote `$ref`. The checker can't reason about them
soundly, and a gate that silently passes what it can't check is worse than no gate.

**Formats are asserted, for a fixed set, by System.Text.Json's rules.** In draft 2020-12,
`format` is only an annotation by default, so a standard validator lets `"format": "uuid"`
accept any string. A message would then pass validation, or be routed by it (§4), and fail
later in `Deserialize<T>`. The profile evaluator therefore asserts the formats the exporter
emits for types whose converters reject bad input: `uuid` (`Guid`), `date-time` (`DateTime`,
`DateTimeOffset`), `date` (`DateOnly`) and `time` (`TimeOnly`).

- It parses them the way System.Text.Json does, not by strict RFC 3339. STJ writes an
  unspecified `DateTime` and every `TimeOnly` without a UTC offset, which RFC 3339 rejects,
  so strict assertion would fail STJ's own output.
- Other formats stay annotations, and `lint` warns about them.
- Contracts that cross languages should prefer `DateTimeOffset`. Its values always carry an
  offset, so they also pass standard validators that assert formats.

**Valid doesn't guarantee deserializable.** Some constraints never reach the schema. The
exporter doesn't emit `int` ranges, for example, so `3000000000` passes `"type": "integer"`
and then overflows an `int`. Such failures stay what they are today: poison at
deserialization, dead-lettered without retries. The conformance check narrows the gap but
doesn't close it.

### Evolution rules: full compatibility, transitively

A **writer** schema describes what a producer emits, and a **reader** schema what a consumer
accepts. Readers ignore unknown properties (open content).

Every revision *N* of a `$id` must be compatible with every earlier revision *E* in both
directions: readers of *E* accept what *N* writes, and readers of *N* accept what *E* writes.

- Both directions, because the rollout is consumers first: new readers see old messages, and
  other teams' readers that haven't upgraded see new ones.
- Transitive, because old producers outlive several revisions: stale images, outbox
  backlogs, dead-letter replays.

| Change within one `$id` | Verdict | Why |
| --- | --- | --- |
| Annotation only (`title`, `description`, `examples`, `deprecated`, `$comment`) | Compatible | No effect on validation |
| `$ref` / `$defs` restructuring | Compatible if equivalent after resolution | Compared structurally |
| Add an optional property | Compatible | Old readers ignore it, and new readers default it |
| Remove an optional property | Compatible | Old writers still send it and readers ignore it. Reusing the name later with another type fails, because the transitive check still compares against the revision that had it. |
| Add a required property | **Breaking** | Old writers don't send it |
| Remove a required property | **Breaking** | Old readers require it |
| Optional → required | **Breaking** | Old writers may omit it |
| Required → optional | **Breaking** | New writers may omit it, and old readers require it. contracts.md doesn't list this case today and should once this ships. |
| Change a type, including `integer` → `number` | **Breaking** | One direction always fails |
| Add or remove `null` | **Breaking** | Old readers reject null, or old writers send it |
| Add or remove an enum value, or an `anyOf` branch | **Breaking** (open question 5) | System.Text.Json throws on an unknown enum name |
| Tighten or loosen a constraint (`minimum`, `maxLength`, `pattern`, `format`) | **Breaking** | One direction always fails |
| Open or close an object (`additionalProperties`) | **Breaking** | A closed reader rejects properties an open writer may add |
| Change a `default` | Warning | It changes meaning, not structure |

Rules apply recursively to nested objects and to array `items`. When a check finds a
breaking change, the message says what to do, for example: "required property `currency`
added: mint a new `$id` (…/v3) and keep …/v2 consumers until drained."

### Conformance: whether services actually agree

Integer versions never had this check:

- **Producer:** the service type's writer schema is included in the canonical one, so every
  message it can emit is valid under the canonical schema.
- **Consumer:** the canonical schema is included in the service type's reader schema,
  ignoring unknown properties. This is checked for `Schema` and for every `$id` in `Reads`.

For .NET types, `conform --assembly` derives both views from the same `JsonTypeInfo`. The
reader view is the exporter's output as is. The writer view marks every property the
serializer always writes as required, and leaves properties with an ignore condition
(`JsonIgnoreCondition.WhenWritingNull`, …) optional. A defaulted parameter is the usual case
where the views differ: readers can do without the property, but the type always writes it.
The canonical file is the reader view, the exporter's output, so the property is optional
there and the producer still conforms, because what it writes is a subset of what the file
allows. For the `Reads` example in §4, the consumer check is what matters: the v2 document,
`legacyCode` included, has to be readable by the v3 type, and it is, because readers ignore
unknown properties. For schemas exported in other languages, one document stands in for both
views. That's an approximation that errs on the safe side for readers.

### Runtime validation (optional, off by default)

This section covers **library validation**, which works the same on every transport. §7
adds the broker's own validation where a transport has one, and covers turning validation
on for every bus at once.

`Runax.Messaging.Contracts.JsonSchema` provides a validating `ISerializer` decorator over
whatever serializer the bus or topic resolves:

```csharp
messaging.AddBus(bus =>
{
    bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
    bus.AddConsumer<OrderPlacedConsumer>();
    bus.ValidateContracts(v =>
    {
        v.OnPublish = ContractValidation.Enforce;  // Off (default) | Report | Enforce
        v.OnConsume = ContractValidation.Report;
    });
});
```

- **Publish.** After the inner `Serialize<T>`, the decorator validates the body against
  `T`'s `Schema` document. `Report` logs and increments
  `runax.messaging.contract.validation_failures` (`direction=publish`). `Enforce` throws
  `ContractValidationException` from `PublishAsync`, before anything reaches the transport
  or the outbox store, since the outbox serializes at enqueue time.
- **Consume.** Before the inner `Deserialize<T>`, the decorator validates against each of
  `T`'s lines, `Schema` first and then `Reads`, and the message passes if any one accepts it.
  CI's conformance check guarantees that every one of those lines is readable by `T`.
  Validating against `Schema` alone would reject the v2 messages that a `Reads` v2 consumer
  exists to accept. A message routed by validation (§4) has already passed this check and
  isn't validated again. `Enforce` treats a failure as poison: it dead-letters immediately
  without retries, because validation is deterministic.
  `x-runax-dlq-exception` names `ContractValidationException`, and the reason lists the
  first few failing JSON pointers.
- Types without `Schema` pass through. A consumer that reads `MessageContext.Body` raw is
  never validated.
- **Cost.** `ISerializer` is string-based, so validation parses the body a second time and
  evaluates it. That's why it's off by default. Suggested use: `Enforce` on publish in tests
  and staging, `Report` on consume in production (sampling is open question 13).

Core needs three small, generic changes for this:

- A serializer decoration hook (`bus.DecorateSerializer(Func<ISerializer, ISerializer>)`),
  applied to whatever the per-topic → per-bus → default resolution picks. Without it, users
  would have to wrap serializers by hand.
- Unsealing `PoisonMessageException`, which is binary-compatible, so that
  `ContractValidationException` can derive from it and reuse the existing
  "dead-letter immediately" path.
- Defining `ContractValidationException` in Abstractions rather than in the JSON Schema
  package. Transports that validate natively (§7) then throw the same type, so a failure
  behaves the same whichever layer caught it.

**Validator engine (open question 6).** The recommendation is a small in-house evaluator
for the contract profile only, the same code the CLI uses. That gives CI and runtime one set
of semantics, no dependency, and AOT safety by construction. The alternatives:

- **JsonSchema.Net** is mature and implements 2020-12. Its documentation doesn't state
  native AOT support for the core package (one companion converter is documented as
  AOT-incompatible), so it would have to pass `tests/Runax.Messaging.AotSmoke` before
  adoption.
- **Corvus.JsonSchema** generates code per schema: fast and AOT-friendly, but adds a codegen
  step.

## 7. Broker-native validation

The goal from §1: reuse a broker's own schema validation where it exists, with library
validation on top. When validation is turned on, the library validates on every bus by
default. A bus whose broker ecosystem validates natively can turn library validation off and
leave validation to the broker.

Two facts make this harder than it sounds:

- What brokers call "schema validation" often doesn't look at the payload.
- The usual way to tell a broker which schema a record uses changes the payload.

This section settles both, then describes how core models native validation without
becoming Kafka-specific.

### Validation levels

Core describes a transport's native validation, per direction, as one of three levels:

| Level | Meaning |
| --- | --- |
| `Payload` | The body is checked against the contract's schema. |
| `Reference` | The broker checks that the message references a schema registered for its topic. The body isn't looked at. |
| `None` | No native validation. |

### What "Kafka's own validator" can mean

| Mechanism | Runs in | Checks | Availability | Notes |
| --- | --- | --- | --- | --- |
| Broker-side schema ID validation | The broker, at produce time | That the record's schema ID is registered under the subject for its topic. Confluent's docs say it does no data introspection. Level: `Reference`. | Confluent Server with a Confluent Enterprise license (Confluent Platform 5.4+), or Confluent Cloud **Dedicated** clusters with the hosted registry. Not other Confluent Cloud cluster types, and not Apache Kafka. | Turned on per topic with `confluent.value.schema.validation=true`. A failing record returns `INVALID_RECORD` to the producer, and **the whole batch is discarded**. Null values pass. |
| Schema Registry JSON Schema serializer | The producing client | The payload against the registered schema. Level: `Payload`. | Any Kafka cluster with a Schema Registry | Java validates only with `json.fail.invalid.schema=true`, and **the default is `false`**. In .NET, `JsonSerializerConfig.Validate` defaults to `true`, but the serializer only accepts typed objects, serializes them with Newtonsoft.Json, and validates with NJsonSchema. |
| Schema Registry JSON Schema deserializer | The consuming client | The payload, when configured | Same as above | Java uses the same `json.fail.invalid.schema` flag. .NET's docs contradict each other: `JsonDeserializerConfig.Validate` defaults to `true`, but the class remarks say no validation against the registered schema is done. This needs a test. |
| Registry compatibility check | The registry, when a schema is registered | A new schema version against earlier ones, per the subject's compatibility level | Same as above | A CI-time check, not a runtime one. See "Reusing the registry in CI" below. |

What this means for "only Kafka validates":

- **Broker-side validation alone never checks a payload.** A record is only as valid as its
  producer's serializer made it.
- **Leaving validation to Kafka is sound only under two conditions.** Every producer on those
  topics validates with its registry serializer, which for Java teams means setting
  `json.fail.invalid.schema=true`. And broker-side validation is on, so a producer that skips
  the registry is rejected.
- **Runax's own producers must validate too.** In Kafka-only mode, the Kafka integration
  validates each outgoing payload against the registered schema itself (below). Turning
  library validation off therefore never means Runax publishes unchecked messages.
- **Broker-side validation always enforces.** `Report` mode can log a library failure and
  still publish, but it can't stop the broker from rejecting a record.
- **Broker-side validation only covers publishing.** On consume, native validation means the
  integration validating client-side.

### The wire-format conflict

A registry serializer has to tell consumers which schema a record uses. Confluent supports
two forms:

| Form | Layout | Fit with Runax |
| --- | --- | --- |
| Payload prefix (the default) | Byte 0 is `0`, bytes 1–4 are the schema ID (big-endian), then the UTF-8 JSON | **Doesn't fit.** The body stops being JSON. 2.x and 1.x Runax consumers would treat every record as a malformed envelope and dead-letter it. Plain-JSON consumers in other languages break. Outbox entries and dead-letter copies would carry binary bodies across transports. |
| Record header | The header value is a version byte `1` followed by the 16-byte schema GUID. The body stays the original JSON. | **Fits.** It sits next to the `runax-*` headers from #71 and leaves the envelope alone. |

Runax-produced records therefore use the header form only. The implementation has to handle
or verify these points:

- **The header value is binary.** `TransportMessage.Headers` carries strings, and the Kafka
  transport writes them as UTF-8. So the Kafka integration writes and reads this header
  itself, outside the generic header slots, and keeps it out of `MessageContext.Headers`. A
  2.x consumer with #71 would show it as a garbled string header, which is harmless.
- **Confluent's docs don't name the header, or the .NET setting.** Java turns the header form
  on with `value.schema.id.serializer=io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer`.
  Third-party clients use `__value_schema_id` and `__key_schema_id` as header names. Both the
  name and confluent-kafka-dotnet's equivalent setting (reportedly
  `JsonSerializerConfig.SchemaIdStrategy`) have to be confirmed against the client's source
  before implementation.
- **The GUID's byte order isn't documented.** The REST API shows GUIDs in 8-4-4-4-12 hex, but
  the docs don't say how the 16 header bytes are ordered. .NET's `Guid.ToByteArray()` is
  mixed-endian, so the add-on must convert explicitly (big-endian, per RFC 9562, unless
  Confluent's clients say otherwise), and it needs an interop test against a Java or Go
  producer.
- **It needs recent versions.** Header IDs need Confluent Cloud or Confluent Platform 8.1.1+
  (Confluent's blog says 8.2). Clients need 2.13.0+ for .NET, Go and Python, 8.1.1+ for Java,
  and 1.8.0+ for JavaScript. Since 8.1.1, Confluent's deserializers look for the header first
  and fall back to the prefix.
- **Broker-side validation of header IDs is unverified.** The broker docs only mention the
  schema ID in the wire format. Confluent's blog lists broker-side validation as supported
  with header IDs, but gives no details. Until that's tested on a Dedicated cluster, Runax
  can't claim `Reference` for header-form records. If the broker reads only the prefix,
  broker-side validation can't be combined with Runax's wire format (open question 18).
- **Some tools drop headers.** Kafka Streams state stores (Confluent Platform 8.2 and
  earlier), ksqlDB, and Kafka Connect unless configured otherwise. Records that pass through
  them lose the schema ID.
- **Foreign prefix-form records can still be consumed.** On receive, the Kafka integration
  can recognize a leading `0` byte plus a 4-byte ID, strip them, and keep the ID. Today such a
  record fails as a malformed envelope. This only applies to reading: Runax never writes the
  prefix.

The registry's GUID and the `$id` are different identifiers:

- The GUID is a fingerprint of the registered schema document, so it changes with every
  revision. It's what the registry looks schemas up by.
- The `$id` is the routing identity (§3).

CI registers each committed file, and the file's body contains its `$id`. That lets the
integration map `$id` → subject → registered version → GUID at startup and cache the result.
The reverse mapping helps too: when a foreign producer used a plain registry serializer and
set no `runax-contract-schema` header, a Kafka consumer can derive `ContractSchema` from the
GUID (open question 20).

**Subjects.** Broker-side validation looks schemas up by subject, which is `<topic>-value` by
default (`TopicNameStrategy`). That gives each topic one compatibility line. A topic that
carries several `$id`s, such as v2 and v3 during a migration or several message types, needs
`RecordNameStrategy` or `TopicRecordNameStrategy`. The strategy has to be set on the broker
topic and in the clients alike, because the broker's setting doesn't propagate to clients
(open question 19).

**Schemas must stay open.** Native validators see the whole body, `__runax` included. A
schema that closes its root object (`additionalProperties: false`) would reject every Runax
message that carries metadata. The Kafka integration strips `__runax` before validating, but
a foreign consumer with a validating deserializer doesn't. So `lint` rejects closed root
objects outright.

### How core models it: a transport capability

Core stays Kafka-agnostic. A transport that can validate natively implements an optional
interface. It's the same approach #71 used to add native headers to `IMessagingTransport`
without breaking custom transports. A sketch; names are to be decided:

```csharp
public enum NativeContractValidation { None, Reference, Payload }

public interface IContractValidatingTransport
{
    // What the transport, and the broker behind it as configured, guarantees per direction.
    NativeContractValidation OnPublish { get; }
    NativeContractValidation OnConsume { get; }

    // Client-side checks. Core calls these when native validation is on for that direction.
    ValueTask<ContractValidationResult> ValidatePublishAsync(TransportMessage message, CancellationToken cancellationToken);
    ValueTask<ContractValidationResult> ValidateReceivedAsync(TransportMessage message, CancellationToken cancellationToken);
}
```

- **Core orchestrates, and the transport only answers.** Core decides between `Report` and
  `Enforce`, records `runax.messaging.contract.validation_failures` with a tag for the layer,
  and throws `ContractValidationException` or dead-letters as poison, exactly as for library
  validation (§6). When a broker rejection surfaces from `PublishAsync`, like Confluent's
  `INVALID_RECORD`, the transport maps it to `ContractValidationException` too.
- **The transport needs the contract even without native headers.** Today a transport only
  sees the `$id` through `runax-contract-schema` in `TransportMessage.Headers`, and that's
  empty when `WriteNativeHeaders = false`. `TransportMessage` gains a `ContractSchema`
  property, so native validation works either way.
- **Levels describe configuration, not hope.** The Kafka integration reports `Payload` on
  publish when it validates client-side. It reports `Reference` only when the user declares
  that broker-side validation is on for the bus's topics, and it can confirm that at startup
  by reading the topic config `confluent.value.schema.validation` through the admin API.
- **Packaging.** The Kafka integration ships as an add-on,
  `Runax.Messaging.Transports.Kafka.SchemaRegistry`, so the plain Kafka transport doesn't
  pull in the registry client, Newtonsoft.Json and NJsonSchema.
  - It isn't AOT-safe, but neither is the Kafka transport today ([aot.md](../aot.md)), so
    nothing regresses.
  - Confluent's .NET serializer only accepts typed objects, so the add-on can't hand it the
    JSON Runax has already produced. Instead it fetches the schema with the registry client
    and validates that JSON with NJsonSchema, the engine Confluent's .NET serializer uses
    (open question 17).

### Configuration: turning validation on everywhere

"Enabled globally" needs a decision, because [configuration.md](../configuration.md) says
there's deliberately no global scope: every setting is per bus.

The recommendation is a **messaging-level default that each bus copies and can override**.
Buses already work this way for JSON options, which start from a copy of the container's
global `JsonSerializerOptions`. Nothing set on one bus leaks into another, so the
configuration.md rule still holds, with this one documented default added.

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    // The default for every bus: library validation on.
    messaging.ValidateContracts(v =>
    {
        v.OnPublish = ContractValidation.Enforce;
        v.OnConsume = ContractValidation.Report;
    });

    // Inherits the default. RabbitMQ has no native validation, so the library validates.
    messaging.AddBus(bus => bus.AddTransport(new RabbitMqConfig { HostName = "rabbit" }));

    // Kafka with Schema Registry: leave validation to Kafka alone.
    messaging.AddBus("events", bus =>
    {
        bus.AddTransport(new KafkaConfig { BootstrapServers = "kafka:9092" });
        bus.UseSchemaRegistry(new SchemaRegistryOptions { Url = "https://registry.acme.dev" });
        bus.ValidateContracts(v => v.Library = false);
    });
});
```

Two switches pick the layers. `v.Library` (default `true`) controls library validation.
`v.Native` (default `true`) uses the transport's validation when it has one. Setting
`v.Native = false` keeps a transport integration's other duties, such as writing registry
IDs, but skips its validation.

For each direction (`OnPublish`, `OnConsume`), validation resolves as:

| Mode for the direction | `v.Library` | Transport's native level | What runs |
| --- | --- | --- | --- |
| `Off` | any | any | Nothing |
| `Report` / `Enforce` | `true` | `None` | Library |
| `Report` / `Enforce` | `true` | `Reference` or `Payload` | Both. Under `Enforce`, either one can reject. |
| `Report` / `Enforce` | `false` | `Payload` | Native only |
| `Report` / `Enforce` | `false` | `Reference` | **Startup error**, unless the bus sets `v.AcceptReferenceOnly = true`. The broker would only check that a schema ID is attached. |
| `Report` / `Enforce` | `false` | `None` | **Startup error.** Validation was asked for, and nothing would validate. |

The last two rows fail at startup rather than warning, because silently weaker validation is
the failure this feature exists to prevent (open question 16).

### Other brokers

| Transport | Native option | What it checks | JSON Schema? | Plan |
| --- | --- | --- | --- | --- |
| Kafka with Confluent | Registry serializers and broker-side ID validation | Client: payload. Broker: reference. | Yes | First implementation (above) |
| Kafka on MSK, and Kinesis | AWS Glue Schema Registry serializers | Payload, client-side | Drafts 04, 06 and 07 only | Not planned. The libraries are Java, they carry the schema version ID in the payload, and our profile is draft 2020-12. |
| Azure Event Hubs | Azure Schema Registry | Client-side only | Listed as a format, but the only .NET serializer is for Avro (`Microsoft.Azure.Data.SchemaRegistry.ApacheAvro`). JSON Schema only has Java/Kafka samples. | `None` until a .NET JSON Schema serializer exists |
| Google Pub/Sub | Topic schemas | **Payload, server-side, at publish.** A failing message is rejected with `INVALID_ARGUMENT`. | **No.** Avro and Protocol Buffers only, though messages can use their JSON encoding. | Deferred. It would need a translation from JSON Schema to Avro or Protobuf, and the docs don't say how Pub/Sub treats the extra `__runax` field. Validated messages arrive with `googclient_schemaname`, `googclient_schemaencoding` and `googclient_schemarevisionid` attributes. |
| Service Bus, SQS, SNS, RabbitMQ, Redis Streams, in-memory | None known | — | — | Library validation only |

### Reusing the registry in CI

On Kafka, "Kafka's own validator" can also mean the registry's compatibility checker. CI can
register each committed file under its subject, with the subject's compatibility level set to
`FULL_TRANSITIVE` to mirror §6, and fail the pipeline if registration fails.
`runax-messaging contracts check` stays the gate, because it covers every transport, including
contracts that never touch Kafka. The two rule sets could disagree, for example on open
versus closed objects, so which one wins is open question 21.

### Sources (checked 2026-10-19)

- Confluent broker-side schema ID validation:
  [Confluent Cloud](https://docs.confluent.io/cloud/current/sr/broker-side-schema-validation.html),
  [Confluent Platform](https://docs.confluent.io/platform/current/schema-registry/schema-validation.html)
- Confluent wire formats and header schema IDs (re-checked 2026-10-20):
  [serdes overview](https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html),
  [blog post](https://www.confluent.io/blog/schema-id-kafka-headers-data-governance/)
- Confluent JSON Schema serdes:
  [Java](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-json.html),
  .NET [`JsonSerializerConfig`](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.SchemaRegistry.Serdes.JsonSerializerConfig.html)
  and [`JsonDeserializer`](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.SchemaRegistry.Serdes.JsonDeserializer-1.html)
- Azure Schema Registry:
  [concepts](https://learn.microsoft.com/azure/event-hubs/schema-registry-concepts),
  [client-side enforcement](https://learn.microsoft.com/azure/event-hubs/schema-registry-client-side-enforcement)
- Pub/Sub:
  [schemas](https://cloud.google.com/pubsub/docs/schemas),
  [publishing to a topic with a schema](https://cloud.google.com/pubsub/docs/publish-topics-schema)
- AWS Glue Schema Registry:
  [overview](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html),
  [how it works](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-works.html)

## 8. Migration from integer versions

### Phases

**Phase 1: a 2.x minor release. Additive only, no wire break.**

- `MessageContractAttribute` gains two init properties, `string? Schema` and
  `string[]? Reads`. The `int` constructor is unchanged, so schema contracts declare both:
  `[MessageContract(2, Name = "orders.placed", Schema = OrderContracts.OrderPlacedV2)]`.
- Publishing also writes `contract_schema` / `runax-contract-schema`.
- Dispatch uses the rule in §4.
- New public members: `MessageContext.ContractSchema`, `UnroutableMessage.ContractSchema`,
  the `IMessageContractCatalog` additions, and `MessageHeaders.ContractSchema` with the new
  priority order.
- New package `Runax.Messaging.Contracts.JsonSchema` (export profile, embedded schema
  provider, validating serializer, self-check) and the `runax-messaging` tool.
- The validation scope (`messaging.ValidateContracts` and the per-bus override) and the
  `IContractValidatingTransport` capability ship here too (§7), so that native validation
  can be added later without touching core again.

**Phase 1b: a later 2.x minor.** The `Runax.Messaging.Transports.Kafka.SchemaRegistry`
add-on (§7). It doesn't change the wire for anyone who doesn't install it. Where it is
installed, it adds the registry header and strips foreign prefixes on receive.

**Phase 2: adoption. This is fleet work, not a release.** For each contract:

1. Commit the canonical document with `x-runax-legacy-version`, and turn on lint, check, and
   conform.
2. Add `Schema` to consumer types and ship the consumers. They now accept the `$id`, and
   the integer too, from the attribute or from the embedded line (§4). Consider
   `ContractSchemaMismatch = Report` on their buses until producers are confirmed to write
   the right `$id` (§4).
3. Ship producers, which then write both automatically. Foreign producers add the
   `runax-contract-schema` header.
4. Run validation in `Report` mode in staging.

**Phase 3: 3.0. Breaking API changes, same wire.**

- `[MessageContract(Schema = …)]` without an integer becomes valid: a new parameterless
  constructor, with `Version` becoming `int?`.
- The `int` constructor, `HandledContract.Version`, `Accepts(string, int)`,
  `MessageContext.ContractVersion` and `UnroutableMessage.ContractVersion` are marked
  `[Obsolete]` (a warning, not an error).
- Dropping the integer from a contract **type** is safe once the service embeds its schema
  documents (§5). The line's file keeps the integer, so the service still writes it as a
  producer and still accepts it as a consumer (§4). A service without embedded documents
  keeps the integer on the type until nothing routes by it: no 2.x consumer of that contract
  (check with the catalog or manifest, or broker metrics), and no int-only messages left in
  DLQs or outbox backlogs.
- The integer leaves the **wire** only when the fleet is done with it: a later release stops
  writing it, and reading it stays supported (open question 9).
- `migrating-to-3.0.md` documents the switch.

### Compatibility during coexistence

"2.y" below is the phase 1 minor release. "2.x" is any earlier 2.x (or 1.x) service.

| Producer ↓ / Consumer → | 2.x, int | 2.y+ with `Schema` + int | 3.0, `Schema` only |
| --- | --- | --- | --- |
| 2.x, int | ✓ int | ✓ int | ✓ int via the line's legacy version¹, else ✗ unroutable |
| 2.y+ with `Schema` + int | ✓ int | ✓ `$id` | ✓ `$id` |
| 3.0, `Schema` only | ✓ int via the line's legacy version¹, else ✗ unroutable | ✓ `$id` | ✓ `$id` |
| Foreign, `runax-contract-version` only | ✓ int | ✓ int | ✓ int via the line's legacy version¹, else ✗ unroutable |
| Foreign, `runax-contract-schema` (+ version) | ✓ int if the version is set | ✓ `$id` | ✓ `$id` |
| Foreign or untyped, **no identity** | ✗ unroutable (as today) | ✓ by validation, if selective and unambiguous | ✓ by validation, if selective and unambiguous |

¹ When the 3.0 service embeds its schema documents (§5), which it already needs for
validation and for routing unidentified messages.

Producers that never set identity, such as plain-JSON services in other languages, only need
their consumers to declare `Schema`. Validation-based routing (§4) then works the same
before and after 3.0.

The remaining ✗ cells only affect 3.0 services that don't embed their documents, and they're
the reason those services keep the integer on their types. They fail safe: the message is
unroutable and dead-lettered with its headers, so it can be replayed once the consumer
catches up. It's never silently misdelivered.

### The rule CI must enforce during coexistence

**A breaking change mints a new `$id` and a new integer together.** 2.x consumers still route
by the integer. If a producer moved to `…/v3` but kept writing `2`, 2.x v2 consumers would
receive v3 shapes, which is exactly the silent mismatch this spec removes. `lint` enforces it
through `x-runax-legacy-version`: it never changes within a `$id`, and it's unique per
contract line.

### What happens to `[MessageContract]`

It stays the single place a contract is declared, and no second attribute is added. It gains
`Schema` and `Reads`. The integer becomes legacy and is deprecated in 3.0. Reading the
integer from the wire stays supported indefinitely: it costs nothing, and it keeps old DLQ
replays and foreign producers working (open question 9). `Name` stays an optional,
non-routing label.

## 9. Implementation outline (non-binding)

1. **Abstractions:**
   - `MessageContractAttribute.Schema` / `Reads`, `MessageHeaders.ContractSchema` and the
     priority order.
   - `MessageContext.ContractSchema` and `InferredContractSchemas`,
     `UnroutableMessage.ContractSchema` and `UnroutableReason`, and
     `TransportMessage.ContractSchema`.
   - Unseal `PoisonMessageException`, and add `ContractValidationException`.
   - The optional `IContractValidatingTransport` interface and its level enum (§7).
2. **Core:**
   - Envelope read/write of `contract_schema`, and `NativeHeaders` build/merge/`FromEnvelope`.
   - `IMessageConsumer` exposes its accepted schemas, and `MatchConsumers` implements §4.
   - Unroutable reason and logs, the span attribute, catalog additions, and the serializer
     decoration hook.
   - The validation scope: a messaging-level default copied into each bus, the bus
     override, the §7 resolution table with its startup errors, and calls into a validating
     transport.
   - The unidentified-message path: the matcher hook that the JSON Schema package
     implements, the ambiguity rule, `RequireContractIdentity`, and the unroutable reasons
     (§4).
   - Legacy integers taken from embedded lines (*V* in §4), for both matching and writing,
     and the `ContractSchemaMismatch` safety valve.
3. **Outbox:** no store change. The envelope already carries the new key, and the dispatcher
   rebuilds native headers through `FromEnvelope`.
4. **`Runax.Messaging.Contracts.JsonSchema`:** export profile and `ContractSerialization`
   attribute, embedded schema provider, profile evaluator, `ValidateContracts`,
   `VerifyContractSchemas`, and the matcher for unidentified messages, including the
   startup overlap warning (§4).
5. **`Runax.Messaging.Cli` (`runax-messaging contracts`):** lint, check, conform, export. Shipped as a .NET tool, native
   binaries, and a container image. Optionally, a `register` command that publishes
   committed files to a Confluent Schema Registry (§7).
6. **`Runax.Messaging.Transports.Kafka.SchemaRegistry` (phase 1b):**
   - `$id` → GUID mapping, and writing the registry header.
   - Client-side validation with NJsonSchema.
   - Stripping foreign prefixes on receive.
   - Mapping broker `INVALID_RECORD` errors.
   - The startup check of `confluent.value.schema.validation`.
7. **Tests:**
   - The §4 matching table as data-driven tests, including unidentified messages. Cover
     one line per topic, overlapping lines with the same consumers, overlapping lines with
     different consumers (ambiguous, with and without an accept-all consumer), no match with
     and without an unversioned consumer, a non-selective line, `RequireContractIdentity`,
     int-only messages to `Schema`-only and `Reads` consumers, and both
     `ContractSchemaMismatch` modes.
   - The asserted formats: values that pass the profile evaluator deserialize under
     System.Text.Json, and ones it rejects don't.
   - 2.x wire fixtures: envelopes with and without the new key, a native-only schema header,
     and a non-integer version.
   - Every row of the evolution table, and every row of the §7 resolution table.
   - The new package in `tests/Runax.Messaging.AotSmoke`.
   - For the Kafka add-on, integration tests against a Schema Registry container. Tests of
     broker-side validation need Confluent Server, so they can't run on Apache Kafka images.
8. **Docs:** contracts.md (including the corrected required → optional rule), headers.md,
   consuming.md, serialization.md, configuration.md (the validation default), the Kafka
   README, CHANGELOG.

## 10. Alternatives considered

| Alternative | Why not |
| --- | --- |
| Put the `$id` in `contract_version` | 2.x readers throw and dead-letter (§2) |
| Fingerprint or semver as the identity | §3 |
| Runtime compatibility matching or registry lookup | §4. It adds a network dependency to routing, and decides compatibility too late. Routing unidentified messages by validation (§4) is different: it checks against the consumer's own embedded schemas, needs no producer schema or network, and only runs when the wire carries no identity. |
| A new `[JsonSchemaContract]` attribute | Two places to declare a contract, and a harder migration. Extending `[MessageContract]` keeps one place. |
| Declare readable ids on the consumer class instead of the type | Invisible to the CLI, which reads types and schemas, not DI registrations. Could be added later as an override. |
| Confluent Schema Registry or AWS Glue in core | Tied to a broker ecosystem. Native validation goes through a transport capability instead, with Confluent as a Kafka add-on (§7). |
| Confluent's payload-prefix wire format | The body stops being JSON, which breaks 2.x consumers, plain-JSON interop, and cross-transport outbox and DLQ copies (§7). Prefixes are only read, never written. |
| Delegate validation to Kafka on the broker alone | Broker-side validation only checks that a schema ID is attached, never the payload (§7) |

## 11. Open questions

Each question has a recommendation, but the decision is yours.

1. **`$id` form.** An `https://` URL under a domain you control, or `urn:runax:…`? An https
   URL can later serve the documents for humans and tools, but the library never needs it
   to resolve. *Recommend https.*
2. **Where canonical schemas live.** One fleet-wide contracts repo, consumed as a git
   submodule or published as NuGet, npm, Go module and Maven artifacts? Or each owner's repo
   publishes its own? *Recommend one contracts repo for a mixed fleet: one review point, one
   gate.*
3. **.NET-owned contracts.** Export and commit, or hand-author and generate C#? *Recommend
   export and commit now, with C# generation as a later option.*
4. **Compatibility level within a `$id`.** Full transitive (adding optional properties is
   about the only freedom), or backward transitive? Backward also allows removing
   properties, making required ones optional, adding enum values and adding `null`, but it
   relies on every consumer upgrading before any producer. *Recommend full transitive.*
5. **Enum growth.** Should adding an enum value always be breaking, or should an annotation
   (`x-runax-open-enum`) make it compatible when readers map unknown values to a fallback?
   *Recommend breaking by default, with the annotation as a follow-up.*
6. **Validator engine.** In-house profile evaluator, JsonSchema.Net (after an AOT check), or
   Corvus? *Recommend in-house.*
7. **Consume-side validation failure.** Dead-letter as poison, or hand it to the unroutable
   strategy? *Recommend poison: the identity was handled, the body was wrong.*
8. **Release vehicle.** Phase 1 in a 2.x minor, or hold everything for 3.0? *Recommend a 2.x
   minor so the fleet can start adopting before the breaking release.*
9. **The integer's future.** Deprecate in 3.0 and keep reading it forever, or remove it in
   4.0? *Recommend deprecating writes and keeping reads.*
10. **Fleet manifest and deploy gate** (`runax-messaging contracts manifest`). In scope for the first
    release? *Recommend a fast follow, not phase 1.*
11. **Revision fingerprint header.** Add an optional `runax-contract-revision` (a hash of the
    canonical revision) for drift telemetry, so consumers can see which revision of a `$id`
    producers are on? *Recommend not in phase 1.*
12. **Several message types per topic.** Now that it routes correctly, document it as
    supported, or keep "one type per topic" guidance? *Recommend documenting it.*
13. **Validation sampling, and TestKit defaults.** Offer a sample rate for consume-side
    `Report`? Turn `Enforce` on by default inside the TestKit harness, so unit tests catch
    drift? *Recommend yes to TestKit, and defer sampling.*
14. **CloudEvents.** When the CloudEvents serializer (roadmap) lands, map `contract_schema`
    to and from `dataschema`? *Recommend yes. It's the reason for choosing an absolute URI.*

Broker-native validation (§7):

15. **"Enabled globally."** Options: a messaging-level default that every bus copies and can
    override, which would be the first messaging-level setting; a per-bus helper
    convention, as configuration.md suggests today; or a plain container options class.
    *Recommend the messaging-level default. It has the same shape as the JSON options
    buses already copy, and a fleet-wide policy is the point.*
16. **Library validation off, but the broker only checks references.** Fail at startup
    unless `AcceptReferenceOnly` is set, or just warn? *Recommend failing.*
17. **What "Kafka validates" means for Runax's own publishes.** Should the add-on validate
    payloads client-side, with the registry's schema and NJsonSchema? Or should Runax rely
    on broker-side ID validation and other producers' serializers? *Recommend client-side
    validation. Otherwise Kafka-only mode publishes unchecked payloads.*
18. **The registry ID form, and whether the broker supports it.** *Recommend header GUIDs
    only.* First test broker-side validation of header IDs on a Dedicated cluster. If the
    broker doesn't support it, either give up broker-side validation on Runax topics, or
    allow a per-topic prefix opt-in that gives up plain-JSON interop. *Recommend giving up
    broker-side validation.*
19. **Subject naming for topics that carry several `$id`s.** Use `TopicRecordNameStrategy`,
    or allow only one `$id` per registry-validated topic? It also needs checking how
    Confluent derives a JSON Schema's record name. *Recommend `TopicRecordNameStrategy`.*
20. **Identity derived from the registry.** When a foreign record has a registry ID but no
    `runax-contract-schema` header, should Kafka consumers derive `ContractSchema` from the
    registry? *Recommend yes, as an opt-in.*
21. **Which compatibility check wins when both run.** *Recommend that `runax-messaging contracts check`
    gates, with the registry set to `FULL_TRANSITIVE` as a second check whose failures also
    fail CI.*
22. **Packaging.** A separate `Runax.Messaging.Transports.Kafka.SchemaRegistry` add-on, or
    part of the Kafka transport? *Recommend the add-on, which keeps the registry client and
    Newtonsoft out of plain Kafka users' dependencies.*
23. **Other brokers.** Pub/Sub would need translation to Avro or Protobuf, and Event Hubs has
    no .NET JSON Schema serializer. Defer native validation for both? *Recommend deferring.
    Library validation already covers them.*

Optional identity and routing by validation (§4):

24. **Default.** Should validation-based routing be on by default, with a per-bus
    `RequireContractIdentity = true` opt-out for strict setups, or opt-in per bus? Today an
    unidentified message never reaches a versioned consumer, so turning it on only turns
    some unroutable messages into deliveries of valid ones. *Recommend on by default,
    whenever the JSON Schema package is installed.*
25. **Ambiguity.** When lines that reach different consumers all accept a message, options
    are: deliver it to no schema consumer and leave accept-all consumers as they are today;
    refuse it for everyone, accept-all consumers included; deliver it to every accepting
    line; or pick the newest line. *Recommend the first. Guessing or double-delivering is
    how mismatches go unnoticed, and refusing it for everyone would let a new schema
    consumer cut off an existing accept-all one.*
26. **The overlap check.** Should two lines on one topic that could accept the same message,
    and that reach different consumers, be a warning or an error, at startup and in CI? The
    check counts lines as disjoint only when a shared required property has disjoint
    `const`/`enum`/`type` values. *Recommend a warning, and documenting a discriminator
    property as the fix.*
27. **Int-only messages and schema-only consumers.** A consumer that declares only `Schema`
    accepts an int-only message when the integer is one of its lines' `x-runax-legacy-version`
    values (§4). Should an integer that maps to none of its lines be validated against its
    schemas instead? *Recommend not. The integer is an identity claim, and one that matches
    nothing the consumer declares is a real mismatch.*
28. **Non-selective lines.** A line with no required property and no `const` discriminator
    accepts every JSON object. Leave it out of routing by validation (its consumers need
    identity), or allow it with a warning? *Recommend leaving it out. Inference by such a
    line isn't routing.*
29. **Adoption safety valve.** Offer `bus.ContractSchemaMismatch = Report`, which delivers a
    message whose `$id` mismatches but whose integer matches, with a warning and a metric?
    Is the default `Reject`? *Recommend offering it, defaulting to `Reject`, and suggesting
    `Report` only while a contract is being adopted.*
