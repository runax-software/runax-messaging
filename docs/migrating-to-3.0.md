# Migrating from 2.x to 3.0

3.0 adds **JSON Schema contracts**: a contract is identified by the `$id` of its schema document
rather than by an integer, because an integer is a label and not a check — two services that both
say "version 2" can disagree on the shape and nothing notices. See
[JSON Schema contracts](json-schema-contracts.md) for the feature itself.

**Most apps compile and run unchanged.** The integer form still works, the envelope and the
dead-letter headers are unchanged, and 2.x and 3.0 services interoperate on the same broker. There
is exactly one change you may have to make, and it only bites if you read
`MessageContractAttribute.Version`.

## What breaks

### `MessageContractAttribute.Version` is `int?`

It was `int`. Reading it into an `int` no longer compiles:

```csharp
// 2.x
int version = typeof(OrderPlaced).GetCustomAttribute<MessageContractAttribute>()!.Version;

// 3.0
int? version = typeof(OrderPlaced).GetCustomAttribute<MessageContractAttribute>()!.Version;
```

It is nullable because a contract identified by a `Schema` has no integer. This is also a binary
break, so recompile rather than dropping the new assembly next to an old one.

### Everything else is additive

`[MessageContract(2)]`, `MessageContext`, `IBus`, consumers, retries, the outbox, the TestKit and
every transport behave as they did. `PoisonMessageException` is no longer sealed, which only widens
what you can do with it.

## What to do next, when you want schemas

Nothing here is required to run 3.0 — adopt it when it is worth it.

1. **Commit a schema document per contract line** and embed it:

   ```xml
   <ItemGroup>
     <EmbeddedResource Include="schemas/**/*.schema.json" />
   </ItemGroup>
   ```

2. **Identify the contract by its `$id`**, dropping the integer:

   ```csharp
   // 2.x
   [MessageContract(2)]
   public sealed record OrderPlaced(Guid OrderId, decimal Total);

   // 3.0
   [MessageContract(Schema = "https://contracts.acme.dev/orders/order-placed/v2")]
   public sealed record OrderPlaced(Guid OrderId, decimal Total);
   ```

   Declaring **both** is a configuration error: the `$id` already names the contract, so a number
   next to it is a second identity nothing keeps in step with the first.

3. **Load the documents**, which turns on routing by validation and the startup warnings:

   ```csharp
   messaging.AddJsonSchemaContracts(c => c.AddEmbeddedSchemas(typeof(OrderPlaced).Assembly));
   ```

4. **Check the committed files from your test suite**, so a breaking edit to a published `$id` fails
   before it is deployed:

   ```csharp
   ContractSchemaLinter.Lint("schemas").Errors.ShouldBeEmpty();
   ContractSchemaCompatibility.Compare("../.baseline/schemas", "schemas").Errors.ShouldBeEmpty();
   ```

### Switching a contract has no overlap period

This is the part to plan. An integer identity and a schema identity do not route to each other: an
integer-only message never reaches a schema consumer, and a `$id` never reaches an integer consumer.
So for one contract, pick one:

- **Move its producers and consumers together**, in one deployment; or
- **Run the two shapes on separate topics** while the rollout proceeds; or
- **Let the producer send no identity at all.** A plain JSON body with no `runax-contract-*` headers
  is routed by validating it against the lines the topic's consumers declare, so a schema consumer
  receives it. This is also how services in other languages interoperate.

Migrating between two *schema* lines is different and does overlap: the reading type declares the
earlier line in `Reads`, so deploy the reader first and move the writers afterwards.

## Removed

These shipped only in the 3.0 development line, so nothing on NuGet used them. Listed in case you
tracked `main`:

| Removed | Instead |
| --- | --- |
| `SchemaMismatch`, `bus.ContractSchemaMismatch` | `Reads`, declared on the type that reads both lines |
| `runax.messaging.contract.schema_mismatch` | — |
| `x-runax-legacy-version` in schema documents | Nothing; a leftover one loads as a plain `x-` annotation |
| `Runax.Messaging.Cli` (`runax-messaging contracts`) | `ContractSchemaLinter` / `ContractSchemaCompatibility`, asserted from your tests |

## Looking ahead

The integer contract version is removed in 4.0. The plan, the full surface that goes, and the open
questions are in
[Removing integer contract versions](specs/removing-integer-contract-versions.md).
