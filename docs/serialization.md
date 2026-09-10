# Serialization & custom serializers

How messages are encoded on the wire, how messages from *other* producers are consumed, and how to customize
the way message bodies are serialized.

## The default wire format

The payload is serialized at the **top level**, and framework metadata (contract, version, headers) is
attached under a single reserved key, `__runax`:

```json
{
  "Id": 1,
  "Name": "widget",
  "__runax": {
    "contract_name": "orders.placed",
    "contract_version": 2,
    "headers": { "traceparent": "00-..." }
  }
}
```

Two consequences fall out of this shape:

- **The envelope is self-identifying.** Presence of `__runax` means "a Runax message"; absence means "not
  ours." There's no guessing.
- **Interop works both ways.** A consumer outside this library sees a normal object and ignores `__runax`; and
  a message produced *outside* this library (which has no `__runax`) is read as a plain body — see below.

Two rules for the default serializer:

- A message type must serialize to a **JSON object** (so `__runax` can sit beside it). Arrays and primitives
  as top-level messages throw at publish time.
- `__runax` is **reserved** — a message type may not declare a property with that name.

## Consuming messages from other producers (no config)

Because a payload without `__runax` is read as-is, you can point a consumer straight at a queue fed by an
external producer — an S3 event notification, another team's service, anything JSON. The whole payload becomes
the body, the message is unversioned, and there are no framework headers:

```csharp
public sealed record S3Event(string Bucket /* , ... */);

public sealed class S3EventConsumer : MessageConsumer<S3Event>
{
    public override string Topic => "s3-events";
    protected override ValueTask HandleAsync(S3Event e, CancellationToken ct) { /* ... */ }
}
```

An S3 notification like `{"Records":[...]}` (or any shape you model) deserializes into your type directly. If
that payload can't be parsed as JSON at all, it's treated as a malformed message and dead-lettered — never
silently dropped.

## Customizing how bodies are serialized

You can change how a message **body** is turned into JSON and back — but not the envelope. The framework always
frames the reserved `__runax` metadata around whatever your serializer produces, so it stays byte-for-byte the
same regardless of which serializer is active. Every Runax message therefore remains self-identifying no matter
how its body was encoded. There are two levels.

### Tweaking the JSON options (most cases)

For a naming policy, converters, or a source-generated `JsonSerializerContext`, configure the bus's
`JsonSerializerOptions` with `bus.ConfigureSerialization(...)` — no custom type required:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
    });
});
```

The options start from a copy of the container's global `JsonSerializerOptions` (the one your app may
already configure for ASP.NET) with your action applied on top, so the bus inherits application-wide
settings and overrides only what it names.

### Replacing the body serializer

To use a different serialization mechanism entirely (a source-generated path, or a third-party library such as
Json.NET), implement `ISerializer`:

```csharp
public interface ISerializer
{
    string Serialize<TMessage>(TMessage message);   // must return a JSON object
    TMessage? Deserialize<TMessage>(string body);   // body has __runax already stripped
}
```

- **`Serialize`** turns a message into a JSON **object** string. The framework attaches `__runax` as a sibling
  key — so your output must be an object (arrays/primitives throw at publish time), and you must not emit a
  `__runax` property yourself (it's reserved and rejected at publish time).
- **`Deserialize`** turns a body — with `__runax` already stripped by the framework — back into your type.

Example on top of Json.NET:

```csharp
public sealed class NewtonsoftSerializer : ISerializer
{
    public string Serialize<TMessage>(TMessage message) => JsonConvert.SerializeObject(message);

    public TMessage? Deserialize<TMessage>(string body) => JsonConvert.DeserializeObject<TMessage>(body);
}
```

Register it on the bus with `bus.UseSerializer<T>()` — it applies to every topic on that bus, and the
`__runax` envelope is unchanged:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.UseSerializer<NewtonsoftSerializer>();
    });
});
```

`UseSerializer<T>()` is resolved from DI, so your serializer can take constructor dependencies. Because it only
controls the body, there is no way for a custom serializer to change or drop the `__runax` envelope — that is
by design.

### Per-bus serialization

Serialization is a **per-bus** setting — there is no global messaging scope. Each bus picks its own
serializer (or JSON options), so two buses can speak different shapes: one broker talks to a system that
needs a different format (say camelCase, or Json.NET) while another bus keeps the defaults. A bus that
configures nothing uses the default `System.Text.Json` serializer with the container's global options.

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
    });

    messaging.AddBus("audit", bus =>
    {
        bus.AddTransport(new SqsConfig { Region = "us-east-1" });
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.UseSerializer<NewtonsoftSerializer>();   // this bus only: a different body serializer
    });
});
```

Buses that want the same settings share a helper `Action<BusBuilder>` applied to each. The `__runax`
envelope is identical on every bus regardless of which serializer is active.

### Per-topic serialization

When the format is a property of the *topic* rather than the bus — one legacy topic keeps snake_case, or a
single topic speaks Avro while everything else is JSON — scope the serializer to that topic with
`bus.UseSerializerForTopic<T>("<topic>")` and `bus.ConfigureSerializationForTopic("<topic>", o => ...)`.

Selection runs from most to least specific, and the first match wins:

1. the topic on this bus — `bus.UseSerializerForTopic<T>("orders")`
2. this bus, any topic — `bus.UseSerializer<T>()`
3. the built-in default — `System.Text.Json` with the container's global options

A per-topic serializer therefore overrides the bus serializer for the same topic, while other topics on that
bus keep the bus (or default) serializer.

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport<KafkaConfig>(c => c.BootstrapServers = "localhost:9092");
        bus.AddConsumer<OrderPlacedConsumer>();

        // Bus default: applies to every topic that nothing more specific overrides.
        bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);

        // The "orders" topic uses a different body serializer on this bus.
        bus.UseSerializerForTopic<AvroSerializer>("orders");

        // The "audit" topic keeps snake_case; every other topic stays camelCase.
        bus.ConfigureSerializationForTopic("audit", o => o.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower);
    });
});
```

Like the bus-level options, a per-topic `ConfigureSerializationForTopic` starts from a copy of the container's
global options and applies your tweaks on top. The `__runax` envelope is identical regardless of which
serializer resolves.
