# JSON Schema contracts

An integer contract version is a label, not a check: two services that both say "version 2"
can disagree on the shape, and nothing notices. JSON Schema contracts identify each contract by
its schema document's `$id`, validate bodies against that schema, and can route messages that
carry no identity at all by the schemas their consumers declare.

> **Status.** This page covers what ships today: identity and routing in `Runax.Messaging`;
> embedded schemas, validation, and routing by validation in `Runax.Messaging.Contracts.JsonSchema`;
> and the `runax-messaging contracts lint` / `check` CI gate in `Runax.Messaging.Cli`. `conform`,
> `export`, a messaging-wide validation default, broker-native validation (Kafka with Confluent Schema
> Registry), and the startup self-check are designed in the [spec](specs/json-schema-contracts.md) but
> not built yet.

## Declaring a schema

`[MessageContract]` gains a `Schema`: the `$id` of the contract's JSON Schema document, an absolute
URI with one value per **breaking line**. Additive changes keep the `$id`; a breaking change mints
a new one (`…/v3`).

```csharp
public static class OrderContracts
{
    public const string OrderPlacedV2 = "https://contracts.acme.dev/orders/order-placed/v2";
    public const string OrderPlacedV3 = "https://contracts.acme.dev/orders/order-placed/v3";
}

[MessageContract(2, Schema = OrderContracts.OrderPlacedV2)]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

Keep the integer while 2.x services still route by it. Publishing then writes both: the `$id` as
`__runax.contract_schema` and as the native header `runax-contract-schema`, and the integer as before.

A type that can read an earlier breaking line says so with `Reads`, and its consumers accept
messages of both lines:

```csharp
// v3 dropped legacyCode, which v2 required: breaking for v2 readers, but a v3 reader just ignores it.
[MessageContract(3, Schema = OrderContracts.OrderPlacedV3, Reads = [OrderContracts.OrderPlacedV2])]
public sealed record OrderPlacedV3(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

`Reads` without a `Schema` is a configuration error.

## Routing

The dispatcher decides per consumer, in this order:

1. **Unversioned consumers** (no `[MessageContract]`) accept every message, as always.
2. **A schema `$id` decides when both sides have one.** The consumer accepts its `Schema` and its
   `Reads`. A mismatched `$id` is rejected even if the integer matches: that's the drift schemas
   exist to catch.
3. **Otherwise the integer decides.** A consumer accepts its type's integer and, when schemas are
   loaded, the `x-runax-legacy-version` each of its lines records. So a v3 consumer that reads v2
   also accepts an integer-only `2` from a 2.x producer.
4. **A message with neither is routed by validation**, when schemas are loaded (below) and the
   bus doesn't set `RequireContractIdentity`. Its body is validated against the lines the topic's
   consumers declare, and it reaches the consumers whose line accepts it.

| Consumer declares | Message carries | Delivered? |
| --- | --- | --- |
| Nothing | Anything | Yes |
| `Schema` (+ `Reads`) | That `$id`, or one in `Reads` | Yes |
| `Schema` + int 2 | Another `$id`, int 2 | No, unless the bus sets `ContractSchemaMismatch = Report` |
| `Schema` + int 2 | No `$id`, int 2 | Yes, by the integer |
| int 2 only | `$id` + int 2 | Yes, by the integer |
| int 2 only | `$id` only | No |
| `Schema` | Nothing | Yes, if the body validates against one of its lines and isn't ambiguous |

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
  "x-runax-legacy-version": 2,
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
startup. Loading the package is what turns on routing by validation and the legacy versions lines
record.

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
current file. The `runax-messaging` tool checks it in CI, before anything is deployed:

```bash
dotnet tool install --global Runax.Messaging.Cli
runax-messaging contracts lint schemas/
runax-messaging contracts check schemas/
```

- **`contracts lint`** checks each file on its own and the files together: a 2020-12 `$schema`, an
  absolute `$id` of at most 256 bytes that matches the file path (`schemas/orders/order-placed/v2.schema.json`
  for `…/orders/order-placed/v2`), only profile keywords (a typo like `requried` is an error; custom
  annotations start with `x-`), an open root object, unique `$id`s, and a distinct
  `x-runax-legacy-version` per line of a contract. It warns about lines that accept any object and
  about formats that aren't asserted.
- **`contracts check`** compares each file with **every** earlier revision of the same `$id` in git
  history, both ways: readers of either must accept what the other writes. Adding or removing an
  optional property is the one freedom; anything else, such as a new required property, a type
  change, or a tightened constraint, is breaking, and the message says which new `$id` to mint. A
  changed `default` is a warning, and the legacy version must never change. `--baseline <dir>`
  compares against another directory instead, such as a checkout of the base branch.

Exit codes are `0` (no errors), `1` (errors found), and `2` (usage problems, such as a missing
directory or no git repository). In GitHub Actions, check out the full history and use
`--format github` to annotate the pull request:

```yaml
- uses: actions/checkout@v5
  with: { fetch-depth: 0 }          # check compares against every earlier revision
- run: dotnet tool install --global Runax.Messaging.Cli
- run: runax-messaging contracts lint schemas/ --format github
- run: runax-messaging contracts check schemas/ --format github
```

## Bus settings

| Setting | Default | Effect |
| --- | --- | --- |
| `RequireContractIdentity` | `false` | `true` turns off routing by validation: unidentified messages only reach unversioned consumers |
| `ContractSchemaMismatch` | `Reject` | `Report` delivers a message whose `$id` mismatches but whose integer matches, with a warning and `runax.messaging.contract.schema_mismatch`. A safety valve while a contract is being adopted |
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
        {Key: "runax-contract-version", Value: []byte("2")}, // only while 2.x consumers remain
    },
}, nil)
```

Generate the Go, Java, or Node types from the same committed schema files (quicktype,
jsonschema2pojo, json-schema-to-typescript) so they can't drift.

## See also

- [Message contracts](contracts.md): designing, sharing, and evolving contract types
- [Headers & interop](headers.md): native broker headers
- [The design spec](specs/json-schema-contracts.md): the full design, including the parts not built yet
