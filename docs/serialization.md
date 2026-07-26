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

For a naming policy, converters, or a source-generated `JsonSerializerContext`, configure the shared
`JsonSerializerOptions` — no custom type required:

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddInMemory(inMemory =>
    {
        inMemory.AddConsumer<OrderPlacedConsumer>();
    });

    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(opt => opt.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
    });

    runax.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
});
```

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

Register it at the top level — it applies to every transport, and the `__runax` envelope is unchanged:

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddInMemory(inMemory =>
    {
        inMemory.AddConsumer<OrderPlacedConsumer>();
    });

    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(opt => opt.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
    });

    runax.UseSerializer<NewtonsoftSerializer>();
});
```

`UseSerializer<T>()` is resolved from DI, so your serializer can take constructor dependencies. Because it only
controls the body, there is no way for a custom serializer to change or drop the `__runax` envelope — that is
by design.

### Per-broker serialization

Both `UseSerializer<T>()` and `ConfigureSerialization(...)` also work **inside a transport block**, scoping the
serializer to that one broker — exactly like `AddConsumer<T>()`. A top-level call sets the global default; a
call inside a transport block overrides it for that broker only. Useful when one broker talks to a system that
needs a different shape (say camelCase, or Json.NET) while the rest of the app keeps the defaults.

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    // Global default: applies to every broker that doesn't override it.
    runax.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);

    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
        // RabbitMQ inherits the global camelCase options.
    });

    runax.AddSqs(sqs =>
    {
        sqs.Configure(o => o.Region = "us-east-1");
        sqs.AddConsumer<OrderPlacedConsumer>();
        sqs.UseSerializer<NewtonsoftSerializer>();   // SQS only: a different body serializer
    });
});
```

A broker's scoped `ConfigureSerialization` starts from a copy of the global options and applies your tweaks on
top, so it inherits global settings and overrides only what it names. The `__runax` envelope is identical on
every broker regardless of which serializer is active.

### Per-topic serialization

When the format is a property of the *topic* rather than the broker — one legacy topic keeps snake_case, or a
single topic speaks Avro while everything else is JSON — scope the serializer to that topic with
`UseSerializerForTopic<T>("<topic>")` and `ConfigureSerializationForTopic("<topic>", o => ...)`. Both exist at the
top level (the topic on every broker) and inside a transport block (the topic on that one broker).

Selection runs from most to least specific, and the first match wins:

1. the topic on this transport — `AddKafka(k => k.UseSerializerForTopic<T>("orders"))`
2. the topic on any transport — `runax.UseSerializerForTopic<T>("orders")`
3. this transport, any topic — `AddKafka(k => k.UseSerializer<T>())`
4. the global default — `runax.UseSerializer<T>()`

A per-topic serializer therefore overrides a per-broker one for the same topic, while other topics on that broker
keep the broker (or global) serializer.

```csharp
builder.Services.AddRunaxMessaging(runax =>
{
    // Global default: applies to every topic that nothing more specific overrides.
    runax.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);

    // The "orders" topic uses a different body serializer on every broker.
    runax.UseSerializerForTopic<AvroSerializer>("orders");

    runax.AddKafka(kafka =>
    {
        kafka.Configure(o => o.BootstrapServers = "localhost:9092");
        kafka.AddConsumer<OrderPlacedConsumer>();
        // On Kafka only, the "audit" topic keeps snake_case; every other Kafka topic stays camelCase.
        kafka.ConfigureSerializationForTopic("audit", o => o.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower);
    });
});
```

Like the per-broker options, a per-topic `ConfigureSerializationForTopic` starts from a copy of the global options
and applies your tweaks on top. The `__runax` envelope is identical regardless of which serializer resolves.
