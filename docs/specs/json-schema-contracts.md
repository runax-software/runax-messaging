# Spec: JSON Schema contracts

| | |
| --- | --- |
| **Status** | Draft for review. Nothing here is implemented. |
| **Date** | 2026-10-19 |
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
- **Source of truth.** A committed schema file per `$id`. .NET-owned contracts export it from
  the C# type with `JsonSchemaExporter`, which is AOT-clean (verified below), and CI fails if
  the file drifts from the type. Go, Java and Node teams either author the file by hand or
  export it from their own types.
- **Gate.** A `runax-contracts` CLI, shipped as a native binary so non-.NET pipelines don't
  need the .NET SDK. It checks that every revision of a `$id` is fully compatible with all
  earlier ones, and that each service's types conform to the committed file.
- **Validation.** An optional `ISerializer` decorator validates on publish and/or consume.
  It's off by default and can either report or enforce.
- **Migration.** Producers write both the integer and the `$id` while they coexist. 2.x
  services keep routing by the integer. The integer constructor is deprecated in 3.0.

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
- No transport changes.

### Non-goals

- Running or depending on a schema registry service, or fetching schemas over the network
  at runtime.
- Avro, Protobuf, or validating non-JSON bodies.
- Integrations with Confluent Schema Registry or AWS Glue. These stay out of the core
  feature, as the roadmap already says: they're tied to one broker ecosystem, and this
  feature has to stay transport-agnostic.

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
| Contract version (`int`) | `__runax.contract_version` | `runax-contract-version` | Legacy, written during coexistence (§7) |

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

A producer in another language just sets the header:

```go
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value:          body,
    Headers: []kafka.Header{
        {Key: "runax-contract-schema", Value: []byte("https://contracts.acme.dev/orders/order-placed/v2")},
        {Key: "runax-contract-version", Value: []byte("2")}, // only while 2.x consumers remain (§7)
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

Sometimes one type can read two breaking lines. For example, v3 only added a required field
that the v3 type defaults. That type can say so:

```csharp
[MessageContract(3, Schema = OrderContracts.OrderPlacedV3, Reads = [OrderContracts.OrderPlacedV2])]
public sealed record OrderPlacedV3(Guid OrderId, string CustomerId, decimal Total, string Currency = "USD");
```

A producer of this type always writes `Schema`. `Reads` only widens which messages a
consumer of the type accepts. CI's conformance check (§6) verifies that every message valid
under each listed `$id` is readable by the type, so `Reads` is a claim that gets checked,
never something the runtime infers.

### The rule

For each consumer, let *S* be its type's `Schema` plus `Reads` (empty without `Schema`), and
*V* its legacy integer (possibly absent). The dispatcher accepts the consumer when:

```csharp
static bool Accepts(IReadOnlySet<string> s, int? v, string? wireSchema, int? wireVersion)
{
    if (s.Count == 0 && v is null)
        return true;                              // unversioned: accepts everything, as today

    if (wireSchema is not null && s.Count > 0)
        return s.Contains(wireSchema);            // both sides have schema ids: they decide

    return wireVersion is not null && v == wireVersion;   // otherwise fall back to the integer
}
```

| Consumer declares | Wire carries | Result |
| --- | --- | --- |
| Nothing (unversioned) | Anything | Delivered (unchanged) |
| `Schema` (+ int) | Same `$id` | Delivered |
| `Schema` (+ int) | A `$id` in `Reads` | Delivered |
| `Schema` + int 2 | Different `$id`, int 2 | **Not delivered.** A mismatched `$id` is exactly the drift this feature exists to catch, so there's no fallback to the integer here. |
| `Schema` + int 2 | No `$id`, int 2 (2.x or foreign producer) | Delivered by the integer |
| `Schema` only (3.0) | No `$id`, int 2 | Not delivered |
| int 2 only (legacy) | `$id` + int 2 (dual-writing producer) | Delivered by the integer |
| int 2 only (legacy) | `$id` only | Not delivered |

Because `$id`s are globally unique, several message types can share a topic and still route
correctly, which today's integer can't do.

### Unroutable messages

- `UnroutableMessage` gains `string? ContractSchema`. `ContractVersion` stays, and is marked
  obsolete in 3.0.
- The unroutable reason names the `$id` when there is one, for example "No registered
  consumer accepts contract schema 'https://…/order-placed/v3' on topic 'order.placed'." It
  appears in `x-runax-dlq-reason` and in the warning log.
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

A later, optional step (open question 10): `runax-contracts manifest` serializes `Handled`
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
language-neutral place. Foreign producers read it to set `runax-contract-version`, .NET
conformance checks that the attribute's integer matches it, and the linter checks that a
`$id` never changes its integer and that two `$id`s of one contract never share one (§7).

### .NET-owned contracts: export and commit

- `runax-contracts export` loads the contracts assembly, finds every
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
   output), and run `runax-contracts conform` against the canonical file.

.NET services that consume contracts owned by other teams use the same `conform` check on
their C# copies. Generating C# from the file is possible later, for example with a Roslyn
source generator over `AdditionalFiles`, which is AOT-friendly because it runs at compile
time. That's open question 3.

### Schemas at runtime

Validation and the self-check need the documents in-process. The
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

### The `runax-contracts` tool

The tool ships as a .NET tool and as a native AOT single-file binary (linux-x64,
linux-arm64, osx-arm64, win-x64) plus a container image, so Go, Java and Node pipelines
don't need the .NET SDK. Apart from `export`, it only reads schema files.

| Command | Checks |
| --- | --- |
| `lint <dir>` | Profile rules: absolute `$id` ≤ 256 bytes, file path matches `$id`, `$schema` is 2020-12, only supported keywords, `x-runax-legacy-version` consistent |
| `check <dir> --history git` | **Evolution:** each changed file against every earlier revision of the same `$id`, read from the file's git history (needs `fetch-depth: 0`). `--baseline <dir>` is an alternative. |
| `conform --canonical <file> --writer <schema>` / `--reader <schema>` | **Conformance:** a service's own types against the canonical file |
| `export --assembly <dll>` | .NET only: writes schema files from `[MessageContract(Schema = …)]` types |

Output is human-readable text plus SARIF, for PR annotations. A nonzero exit fails the job.

```yaml
- uses: actions/checkout@v5
  with: { fetch-depth: 0 }          # `check` compares against every earlier revision
- run: runax-contracts lint schemas/
- run: runax-contracts check schemas/ --history git
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
(`JsonIgnoreCondition.WhenWritingNull`, …) optional. This matters for the `Reads` example
in §4: the v3 type's `currency` parameter has a default, so a v2 message without it is
readable, yet the type always writes it, so it still conforms as a v3 producer. For schemas
exported in other languages, one document stands in for both views. That's an
approximation that errs on the safe side for readers.

### Runtime validation (optional, off by default)

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
- **Consume.** Before the inner `Deserialize<T>`, the decorator validates against `T`'s
  schema. That's the consumer's own type, which is what has to be readable, so a `Reads`
  alias is validated against the reader's schema. `Enforce` treats a failure as poison: it
  dead-letters immediately without retries, because validation is deterministic.
  `x-runax-dlq-exception` names `ContractValidationException`, and the reason lists the
  first few failing JSON pointers.
- Types without `Schema` pass through. A consumer that reads `MessageContext.Body` raw is
  never validated.
- **Cost.** `ISerializer` is string-based, so validation parses the body a second time and
  evaluates it. That's why it's off by default. Suggested use: `Enforce` on publish in tests
  and staging, `Report` on consume in production (sampling is open question 13).

Core needs two small, generic changes for this:

- A serializer decoration hook (`bus.DecorateSerializer(Func<ISerializer, ISerializer>)`),
  applied to whatever the per-topic → per-bus → default resolution picks. Without it, users
  would have to wrap serializers by hand.
- Unsealing `PoisonMessageException`, which is binary-compatible, so that
  `ContractValidationException` can derive from it and reuse the existing
  "dead-letter immediately" path.

**Validator engine (open question 6).** The recommendation is a small in-house evaluator
for the contract profile only, the same code the CLI uses. That gives CI and runtime one set
of semantics, no dependency, and AOT safety by construction. The alternatives:

- **JsonSchema.Net** is mature and implements 2020-12. Its documentation doesn't state
  native AOT support for the core package (one companion converter is documented as
  AOT-incompatible), so it would have to pass `tests/Runax.Messaging.AotSmoke` before
  adoption.
- **Corvus.JsonSchema** generates code per schema: fast and AOT-friendly, but adds a codegen
  step.

## 7. Migration from integer versions

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
  provider, validating serializer, self-check) and the `runax-contracts` tool.

**Phase 2: adoption. This is fleet work, not a release.** For each contract:

1. Commit the canonical document with `x-runax-legacy-version`, and turn on lint, check, and
   conform.
2. Add `Schema` to consumer types and ship the consumers. They now accept the `$id` and,
   while the type keeps its integer, the integer too.
3. Ship producers, which then write both automatically. Foreign producers add the
   `runax-contract-schema` header.
4. Run validation in `Report` mode in staging.

**Phase 3: 3.0. Breaking API changes, same wire.**

- `[MessageContract(Schema = …)]` without an integer becomes valid: a new parameterless
  constructor, with `Version` becoming `int?`.
- The `int` constructor, `HandledContract.Version`, `Accepts(string, int)`,
  `MessageContext.ContractVersion` and `UnroutableMessage.ContractVersion` are marked
  `[Obsolete]` (a warning, not an error).
- A team drops the integer from a contract type only once nothing still routes by it: no
  2.x consumer of that contract (check with the catalog or manifest, or broker metrics) and
  no int-only messages left in DLQs or outbox backlogs. Until then the type keeps both.
- `migrating-to-3.0.md` documents the switch.

### Compatibility during coexistence

"2.y" below is the phase 1 minor release. "2.x" is any earlier 2.x (or 1.x) service.

| Producer ↓ / Consumer → | 2.x, int | 2.y+ with `Schema` + int | 3.0, `Schema` only |
| --- | --- | --- | --- |
| 2.x, int | ✓ int | ✓ int | ✗ unroutable |
| 2.y+ with `Schema` + int | ✓ int | ✓ `$id` | ✓ `$id` |
| 3.0, `Schema` only | ✗ unroutable | ✓ `$id` | ✓ `$id` |
| Foreign, `runax-contract-version` only | ✓ int | ✓ int | ✗ unroutable |
| Foreign, `runax-contract-schema` (+ version) | ✓ int if the version is set | ✓ `$id` | ✓ `$id` |

The ✗ cells are the reason for the "drop the integer last" rule. They fail safe: the message
is unroutable and dead-lettered with its headers, so it can be replayed once the consumer
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

## 8. Implementation outline (non-binding)

1. **Abstractions:**
   - `MessageContractAttribute.Schema` / `Reads`, `MessageHeaders.ContractSchema` and the
     priority order.
   - `MessageContext.ContractSchema`, `UnroutableMessage.ContractSchema`.
   - Unseal `PoisonMessageException`.
2. **Core:**
   - Envelope read/write of `contract_schema`, and `NativeHeaders` build/merge/`FromEnvelope`.
   - `IMessageConsumer` exposes its accepted schemas, and `MatchConsumers` implements §4.
   - Unroutable reason and logs, the span attribute, catalog additions, and the serializer
     decoration hook.
3. **Outbox:** no store change. The envelope already carries the new key, and the dispatcher
   rebuilds native headers through `FromEnvelope`.
4. **`Runax.Messaging.Contracts.JsonSchema`:** export profile and `ContractSerialization`
   attribute, embedded schema provider, profile evaluator, `ValidateContracts`, and
   `VerifyContractSchemas`.
5. **`runax-contracts`:** lint, check, conform, export. Shipped as a .NET tool, native
   binaries, and a container image.
6. **Tests:**
   - The §4 matching table as data-driven tests.
   - 2.x wire fixtures: envelopes with and without the new key, a native-only schema header,
     and a non-integer version.
   - Every row of the evolution table.
   - The new package in `tests/Runax.Messaging.AotSmoke`.
7. **Docs:** contracts.md (including the corrected required → optional rule), headers.md,
   consuming.md, serialization.md, CHANGELOG.

## 9. Alternatives considered

| Alternative | Why not |
| --- | --- |
| Put the `$id` in `contract_version` | 2.x readers throw and dead-letter (§2) |
| Fingerprint or semver as the identity | §3 |
| Runtime compatibility matching or registry lookup | §4. It adds a network dependency to routing, and decides compatibility too late |
| A new `[JsonSchemaContract]` attribute | Two places to declare a contract, and a harder migration. Extending `[MessageContract]` keeps one place. |
| Declare readable ids on the consumer class instead of the type | Invisible to the CLI, which reads types and schemas, not DI registrations. Could be added later as an override. |
| Confluent Schema Registry or AWS Glue in core | Tied to a broker ecosystem. Possible as separate integrations later (roadmap). |

## 10. Open questions

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
10. **Fleet manifest and deploy gate** (`runax-contracts manifest`). In scope for the first
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
