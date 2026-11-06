# JSON Schema contracts

An integer contract version is a label, not a check: two services that both say "version 2"
can disagree on the shape, and nothing notices. JSON Schema contracts identify each contract by
its schema document's `$id`, validate bodies against that schema, and can route messages that
carry no identity at all by the schemas their consumers declare.

> **Status.** This page covers what ships today: identity and routing in `Runax.Messaging`; and
> embedded schemas, validation, routing by validation, and the lint and compatibility checks in
> `Runax.Messaging.Contracts.JsonSchema`. Conformance and export of schemas from types, a
> messaging-wide validation default, broker-native validation (Kafka with Confluent Schema Registry),
> and the startup self-check are designed in the [spec](specs/json-schema-contracts.md) but not built
> yet.

## Declaring a schema

`[MessageContract]` takes a `Schema`: the `$id` of the contract's JSON Schema document, an absolute
URI with one value per **breaking line**. Additive changes keep the `$id`; a breaking change mints
a new one (`…/v3`). The `$id` is the identity — there is no integer alongside it.

```csharp
public static class OrderContracts
{
    public const string OrderPlacedV2 = "https://contracts.acme.dev/orders/order-placed/v2";
    public const string OrderPlacedV3 = "https://contracts.acme.dev/orders/order-placed/v3";
}

[MessageContract(Schema = OrderContracts.OrderPlacedV2)]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

Publishing writes the `$id` as `__runax.contract_schema` and as the native header
`runax-contract-schema`.

Declaring an integer version as well — `[MessageContract(2, Schema = …)]` — is a configuration error:
the `$id` already names the contract, so a number next to it is a second identity that nothing keeps
in step with the first. The integer form, `[MessageContract(2)]`, is only for contracts that have not
adopted schemas, and is removed in the next major release; see
[Removing integer contract versions](specs/removing-integer-contract-versions.md).

A type that can read an earlier breaking line says so with `Reads`, and its consumers accept
messages of both lines:

```csharp
// v3 dropped legacyCode, which v2 required: breaking for v2 readers, but a v3 reader just ignores it.
[MessageContract(Schema = OrderContracts.OrderPlacedV3, Reads = [OrderContracts.OrderPlacedV2])]
public sealed record OrderPlacedV3(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

`Reads` is how a line is migrated: deploy the reader that accepts both, then move the writers.
`Reads` without a `Schema` is a configuration error.

## Routing

The dispatcher decides per consumer, in this order:

A consumer declares one identity or none, and the dispatcher decides per consumer:

1. **Unversioned consumers** (no `[MessageContract]`) accept every message, as always.
2. **A schema consumer is reached by its `$id`.** It accepts its `Schema` and its `Reads`. A
   mismatched `$id` is rejected: that's the drift schemas exist to catch. An integer on the wire says
   nothing about a schema contract, so it is not a fallback.
3. **An integer consumer is reached by its integer**, and only by that.
4. **A message carrying no identity is routed by validation**, when schemas are loaded (below) and
   the bus doesn't set `RequireContractIdentity`. Its body is validated against the lines the topic's
   consumers declare, and it reaches the consumers whose line accepts it.

| Consumer declares | Message carries | Delivered? |
| --- | --- | --- |
| Nothing | Anything | Yes |
| `Schema` (+ `Reads`) | That `$id`, or one in `Reads` | Yes |
| `Schema` | Another `$id` | No |
| `Schema` | int 2, no `$id` | No — a schema contract has no integer |
| `Schema` | Nothing | Yes, if the body validates against one of its lines and isn't ambiguous |
| int 2 | int 2 (with or without a `$id`) | Yes |
| int 2 | `$id` only | No |

Because `$id`s are globally unique, several message types can share a topic and still route to
the right consumers.

### Messages without identity

A producer doesn't have to say which contract it sent. A plain JSON message from a Go, Java, or
Node service, with no `runax-contract-*` headers, is validated against the candidate lines:

- **Only selective lines take part.** A line that accepts the empty object `{}` would accept any
  JSON object, so it can't tell messages apart. Its consumers need identity on the wire. Require
  a property, ideally a type discriminator, to make a line selective.
- **Ambiguity isn't guessed.** Consumers ignore unknown properties, so a v2 body often also passes
  the v1 schema. When the lines that accept a body reach different consumers, no schema consumer
  gets it. Unversioned consumers still do, and
  `runax.messaging.contract.ambiguous` counts it.
- **Nothing accepts it** → it's unroutable, and the dead-letter reason lists the first failure per
  line, for example `…/order-placed/v3 at /currency: is required`.
- `MessageContext.InferredContractSchemas` lists the lines the body matched;
  `MessageContext.ContractSchema` stays `null`, because the producer claimed nothing.

`UnroutableMessage.Reason` tells custom handlers which case they got: `NoMatchingContract`,
`NoMatchingSchema`, or `AmbiguousSchema`.

## Loading schemas

Commit one document per `$id` (draft 2020-12), embed them, and load them once:

```xml
<ItemGroup>
  <EmbeddedResource Include="schemas/**/*.schema.json" />
</ItemGroup>
```

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://contracts.acme.dev/orders/order-placed/v2",
  "type": "object",
  "properties": {
    "kind": { "const": "order.placed" },
    "orderId": { "type": "string", "format": "uuid" },
    "total": { "type": "number", "minimum": 0 },
    "currency": { "type": "string", "minLength": 3, "maxLength": 3 }
  },
  "required": ["kind", "orderId", "total", "currency"]
}
```

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddJsonSchemaContracts(c => c.AddEmbeddedSchemas(typeof(OrderPlaced).Assembly));
    messaging.AddBus(bus => { /* ... */ });
});
```

`AddSchema(json)` adds a document from text. Two different documents with one `$id` fail at
startup. Loading the package is what turns on routing by validation.

### The contract profile

Documents may use `type` (including `null` unions), `properties`, `required`,
`additionalProperties`, `minProperties`/`maxProperties`, `items`, `minItems`/`maxItems`,
`uniqueItems`, `enum`, `const`, `format`, `anyOf`, `oneOf`, local `$ref` into `$defs` or the
document, and the numeric and string constraints (`minimum`, `exclusiveMaximum`, `multipleOf`,
`minLength`, `pattern`, …). Annotations (`title`, `description`, `default`, `examples`, `x-*`) are
ignored. `if`/`then`/`else`, `not`, `allOf`, `patternProperties`, `dependentSchemas`,
`unevaluated*`, nested `$id`s, and remote `$ref`s fail to load: a validator that silently ignores
a keyword is worse than none.

**Formats** are asserted for `uuid`, `date-time`, `date`, and `time`, with System.Text.Json's own
parsing rules, so a value that validates also deserializes into `Guid`, `DateTime(Offset)`,
`DateOnly`, or `TimeOnly`. Every other format is an annotation. Keep objects open (no
`additionalProperties: false` at the root), so readers ignore properties newer writers add.

## Validating bodies

Validation is per bus and off by default:

```csharp
messaging.AddBus(bus =>
{
    bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
    bus.AddConsumer<OrderPlacedConsumer>();
    bus.ValidateContracts(v =>
    {
        v.OnPublish = ContractValidation.Enforce;   // Off (default) | Report | Enforce
        v.OnConsume = ContractValidation.Report;
    });
});
```

- **Publish** validates the serialized body against the type's `Schema`. `Enforce` throws
  `ContractValidationException` from `PublishAsync` before anything reaches the transport or the
  outbox.
- **Consume** validates the incoming body against the type's `Schema` and `Reads` lines; it
  passes if any one accepts it. `Enforce` makes a failure poison: the message is dead-lettered
  without retries, with `x-runax-dlq-exception` naming `ContractValidationException` and the
  reason listing the failing JSON pointers.
- **`Report`** logs a warning and records `runax.messaging.contract.validation_failures` (tagged
  with the bus, schema, and direction), then carries on.
- Types without a `Schema` aren't validated. A schema that isn't loaded is skipped with a warning.
- Validation parses each body a second time, which is why it's off by default. A reasonable start
  is `Enforce` on publish in tests and staging, and `Report` on consume in production.

`ValidateContracts` is built on `bus.DecorateSerializer(...)`, a general hook that wraps whichever
body serializer the bus resolves for a topic.

## Startup warnings

When the host starts, the package warns, per bus and topic, about:

- a line a consumer declares that no loaded document covers;
- a line that accepts any JSON object (not selective);
- two lines that reach different consumers and could both accept the same message. Lines count as
  distinct only when both require a top-level property whose `const`, `enum`, or `type` values
  don't intersect, so add a required discriminator to clear the warning.

## The CI gate

Exact `$id` matching is only safe if every revision under one `$id` stays compatible with all the
earlier ones, so `…/v2` keeps meaning one shape. A running service can't check that: it only has the
current file. `Runax.Messaging.Contracts.JsonSchema` exposes the two checks, so you assert over your
committed schema files from your own test suite and they run wherever your tests do:

```csharp
[Fact]
public void Schemas_pass_the_contract_profile() =>
    ContractSchemaLinter.Lint("schemas").Errors.ShouldBeEmpty();

[Fact]
public void Schemas_stay_compatible_with_the_base_branch() =>
    ContractSchemaCompatibility.Compare("../.baseline/schemas", "schemas").Errors.ShouldBeEmpty();
```

Both return a `ContractSchemaReport`: `Errors` is what to assert is empty, `Warnings` is worth
reading, and `ToString()` renders one finding per line so a failure says what was wrong. Each
`ContractSchemaFinding` carries the `File`, the `JsonPointer` into the schema, a `Severity`, and a
`Message`.

- **`ContractSchemaLinter.Lint(directory)`** checks each file on its own and the files together: a
  2020-12 `$schema`, an absolute `$id` of at most 256 bytes that matches the file path
  (`schemas/orders/order-placed/v2.schema.json` for `…/orders/order-placed/v2`), only profile
  keywords (a typo like `requried` is an error; custom annotations start with `x-`), an open root
  object, and unique `$id`s. It warns about lines that accept any object and about formats that
  aren't asserted. It runs the same schema compiler the runtime does, so it rejects exactly what the
  runtime rejects.
- **`ContractSchemaCompatibility.Compare(baseline, current)`** compares each file with the file
  declaring the same `$id` in a baseline directory — a checkout of the base branch, say — both ways:
  readers of either must accept what the other writes. Adding or removing an optional property is
  the one freedom; anything else, such as a new required property, a type change, or a tightened
  constraint, is breaking, and the message says which new `$id` to mint. A changed `default` is a
  warning. A line with no counterpart in the baseline is new and has no history to break; one that
  has been retired isn't reported.

Getting a baseline in GitHub Actions is one extra checkout:

```yaml
- uses: actions/checkout@v5
- uses: actions/checkout@v5                  # the base branch, for the compatibility check
  with:
    ref: ${{ github.event.pull_request.base.sha }}
    path: .baseline
- run: dotnet test
```

> **Compatibility isn't transitive.** A revision can be compatible with the one before it and still
> break an older one, whose writers may still be running, so a single baseline is a floor rather
> than a proof. `ContractSchemaCompatibility.Compare(earlier, current, label)` takes two
> `ContractSchemaDocument`s, so if you can enumerate a line's full history — from version control,
> or from an archive of what you published — loop over it and compare against every revision.

## Bus settings

| Setting | Default | Effect |
| --- | --- | --- |
| `RequireContractIdentity` | `false` | `true` turns off routing by validation: unidentified messages only reach unversioned consumers |
| `ValidateContracts(...)` | off | Library validation, above |

## From other languages

Set the headers to route cheaply and unambiguously, or send nothing and let the consumer's schemas
decide:

```go
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value:          body,
    Headers: []kafka.Header{ // optional
        {Key: "runax-contract-schema", Value: []byte("https://contracts.acme.dev/orders/order-placed/v2")},
    },
}, nil)
```

Generate the Go, Java, or Node types from the same committed schema files (quicktype,
jsonschema2pojo, json-schema-to-typescript) so they can't drift.

## See also

- [Message contracts](contracts.md): designing, sharing, and evolving contract types
- [Headers & interop](headers.md): native broker headers
- [The design spec](specs/json-schema-contracts.md): the full design, including the parts not built yet
