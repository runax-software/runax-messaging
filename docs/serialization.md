# Serialization & custom serializers

How messages are encoded on the wire, how messages from *other* producers are consumed, and how to plug in
your own format.

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

## Writing a custom serializer

You only need one for a *different* wire format — a non-Runax envelope such as CloudEvents, or emitting bare
JSON with no `__runax` at all (e.g. when a downstream consumer rejects unknown fields). Implement
`IMessageSerializer`:

```csharp
public interface IMessageSerializer
{
    string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers);
    MessageContext Deserialize(string payload, string topic);
    string EnrichHeaders(string payload, IReadOnlyDictionary<string, string> headers);
}
```

- **`Serialize`** encodes a message (and optional headers) to the wire string.
- **`Deserialize`** decodes a received payload into a `MessageContext` — set `Body` (a JSON string that
  `MessageContext.Deserialize<T>()` reads), plus `Headers` and, if your format carries them, `ContractName` /
  `ContractVersion`. Set `SerializerOptions` so the body is deserialized with your options.
- **`EnrichHeaders`** is called only when dead-lettering, to add `x-runax-dlq-*` headers. Return the payload
  unchanged if your format can't carry headers.

Serializers here are JSON-oriented: the `Body` you hand back is a JSON string.

### Example: bare JSON, no envelope

A serializer that emits/reads just the message — no metadata, no headers, no versioning. Useful for a topic
shared with a strict external consumer:

```csharp
public sealed class RawJsonSerializer(JsonSerializerOptions options) : IMessageSerializer
{
    public string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers) =>
        JsonSerializer.Serialize(message, options);

    public MessageContext Deserialize(string payload, string topic) => new()
    {
        Topic = topic,
        Body = payload,
        Headers = new Dictionary<string, string>(),
        SerializerOptions = options,
    };

    public string EnrichHeaders(string payload, IReadOnlyDictionary<string, string> headers) => payload;
}
```

### Registering it

```csharp
builder.Services.AddRunaxMessaging(messaging => messaging
    .AddSqs(sqs => sqs.Configure(o => o.Region = "us-east-1"))
    .UseSerializer<RawJsonSerializer>()
    .AddConsumer<S3EventConsumer>());
```

`UseSerializer<T>()` replaces the default serializer for the application. It's resolved from DI, so your
serializer can take constructor dependencies (the configured `JsonSerializerOptions` is available). Because the
default already reads foreign JSON, reach for a custom serializer only when the *format itself* differs.

> Serializer selection is currently per-application. Per-topic / per-broker selection is a possible future
> addition — most interop cases are already covered by the default reading raw payloads.
