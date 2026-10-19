# Native AOT & trimming

Runax.Messaging can run in trimmed and native AOT applications (`PublishTrimmed` /
`PublishAot`). Every shipped package is built with `IsAotCompatible`, so the trim and AOT
analyzers check the library's own code on every build. A CI smoke test also publishes a real
app with native AOT and runs it (see [How support is verified](#how-support-is-verified)).

Two things are needed from your side: **source-generated JSON** for your message types, and
**binding configuration in your own code** instead of through the `IConfiguration` overloads.

## Supported packages

| Package | Native AOT | Notes |
| --- | --- | --- |
| `Runax.Messaging.Abstractions`, `Runax.Messaging` | Yes | Including the in-memory transport |
| `Runax.Messaging.Outbox` | Yes | |
| `Runax.Messaging.TestKit` | Yes | |
| `Runax.Messaging.Transports.RabbitMq` | Yes | |
| `Runax.Messaging.Transports.Aws.Sqs` / `.Aws.Sns` | Yes | |
| `Runax.Messaging.Transports.Azure.ServiceBus` | Yes | |
| `Runax.Messaging.Transports.Azure.EventHubs` | Yes | |
| `Runax.Messaging.Transports.Redis` | Yes | |
| `Runax.Messaging.Transports.Kafka` | **No** | `Confluent.Kafka` uses reflection the trimmer can't analyze |
| `Runax.Messaging.Transports.Google.PubSub` | **No** | `Google.Cloud.PubSub.V1` (through Gax, Protobuf and Newtonsoft.Json) uses reflection and runtime code generation |

For the two unsupported transports, the Runax code is annotated and analyzer-clean like
every other package. The broker SDK underneath produces trim/AOT warnings when you publish,
and those warnings point at real runtime failures. Treat those transports as JIT-only until
their SDKs become AOT-compatible.

## Source-generated JSON

Native AOT turns off reflection-based System.Text.Json, so message bodies can only be
(de)serialized through a source-generated `JsonSerializerContext`. Declare one with every
message type you publish or consume:

```csharp
[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(InvoiceIssued))]
internal sealed partial class AppJsonContext : JsonSerializerContext;
```

Then register it. To cover every bus, add it to the container's global
`JsonSerializerOptions`, which the default serializer and every bus-level options copy start
from:

```csharp
builder.Services.Configure<JsonSerializerOptions>(o => o.TypeInfoResolverChain.Insert(0, AppJsonContext.Default));
```

To cover a single bus or topic, use the usual
[serialization settings](serialization.md#tweaking-the-json-options-most-cases):

```csharp
messaging.AddBus(bus =>
{
    bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
    bus.AddConsumer<OrderPlacedConsumer>();
    bus.ConfigureSerialization(o => o.TypeInfoResolver = AppJsonContext.Default);
});
```

If a type is missing from the context, publishing or consuming it throws
`NotSupportedException` ("JsonTypeInfo metadata for type '…' was not provided"). A consumed
message that fails this way goes through the bus's retry policy and is then dead-lettered.

The `__runax` envelope needs no metadata from you: the framework builds it with `JsonNode`.

A custom body serializer registered with `bus.UseSerializer<T>()` has to be AOT-safe itself.
The Json.NET example in [Serialization](serialization.md#replacing-the-body-serializer) is
**not**.

## Configuration binding

`bus.AddTransport<TConfig>(IConfiguration)` and `bus.AddOutboxStore<TConfig>(IConfiguration)`
bind through reflection, so they carry `[RequiresUnreferencedCode]` /
`[RequiresDynamicCode]` and produce a warning in a trimmed or AOT app. Bind the config in your
own code instead, and turn on the
[configuration binding source generator](https://learn.microsoft.com/dotnet/core/extensions/configuration-generator)
so the bind is generated at compile time. `PublishAot` and `PublishTrimmed` turn the generator
on by default:

```csharp
var auditSqs = builder.Configuration.GetSection("Messaging:AuditSqs").Get<SqsConfig>() ?? new SqsConfig();

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus("audit", bus => bus.AddTransport(auditSqs));
});
```

The instance and delegate overloads (`AddTransport(config)`, `AddTransport<TConfig>(c => ...)`)
are AOT-safe.

## What the library takes care of

- **DataAnnotations validation keeps working.** `TransportConfig` is annotated so the public
  properties of every derived config survive trimming, which keeps the validation of your
  `[Required]` / `[Range]` settings intact. That covers [custom transports](writing-a-custom-transport.md)
  too. `RetryOptions` and `OutboxOptions` use the source-generated options validator.
- **DI registrations are annotated.** `AddConsumer<T>()`, `UseSerializer<T>()`,
  `OnUnroutableMessage<T>()`, and the TestKit's `AddService<TService, TImplementation>()` keep
  your types' constructors, so the container can still create them.
- **`[MessageContract]` versioning works unchanged.** The attribute is read from the message
  type, and trimming keeps attributes on any type it keeps.

## How support is verified

[`tests/Runax.Messaging.AotSmoke`](../tests/Runax.Messaging.AotSmoke) is a console app that
references every supported package. In CI, the `build-and-test` job publishes it with native
AOT, warnings as errors and per-assembly warnings expanded (`TrimmerSingleWarn=false`). A trim
or AOT warning in any of those packages or the SDKs they pull in fails the build. The job then
runs the binary, which drives a real round trip: a contract-versioned message delivered on
retry, an outbox-routed publish, and a TestKit harness run, all over the in-memory
transport, with every supported transport compiled in.

To run it locally (needs the
[native AOT prerequisites](https://learn.microsoft.com/dotnet/core/deploying/native-aot/#prerequisites)):

```bash
dotnet publish tests/Runax.Messaging.AotSmoke -c Release -o artifacts/aot-smoke && ./artifacts/aot-smoke/Runax.Messaging.AotSmoke
```
