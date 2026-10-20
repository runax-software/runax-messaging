# Runax.Messaging.Contracts.JsonSchema

JSON Schema contracts for [Runax.Messaging](https://github.com/runax-software/runax-messaging): load
your committed contract schemas, validate message bodies against them on publish and consume, and
route messages that carry no contract identity by the schemas their consumers declare.

## Install

```bash
dotnet add package Runax.Messaging.Contracts.JsonSchema
```

## Use

Declare each contract's schema `$id` on its type:

```csharp
[MessageContract(2, Schema = "https://contracts.acme.dev/orders/order-placed/v2")]
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total, string Currency);
```

Embed the committed schema files and load them:

```xml
<ItemGroup>
  <EmbeddedResource Include="schemas/**/*.schema.json" />
</ItemGroup>
```

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddJsonSchemaContracts(c => c.AddEmbeddedSchemas(typeof(OrderPlaced).Assembly));

    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.ValidateContracts(v =>
        {
            v.OnPublish = ContractValidation.Enforce;
            v.OnConsume = ContractValidation.Report;
        });
    });
});
```

Loading schemas turns on:

- **Routing by validation.** A message with no `$id` and no version is delivered to the consumers
  whose schema its body matches. Opt out per bus with `RequireContractIdentity = true`.
- **Legacy versions.** Consumers accept the `x-runax-legacy-version` each of their schema lines
  records, so integer-only producers keep working.
- **Startup warnings** for missing documents, schemas that accept any object, and overlapping
  schemas on one topic.

Validation uses a built-in evaluator for the contract profile of JSON Schema draft 2020-12. It has
no dependencies and is native-AOT-safe. See the
[JSON Schema contracts guide](../../docs/json-schema-contracts.md).

## License

MIT
