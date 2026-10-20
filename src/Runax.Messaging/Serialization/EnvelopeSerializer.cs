using System.Collections.ObjectModel;
using System.Text.Json;
using System.Text.Json.Nodes;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Framework-owned <see cref="IMessageSerializer"/>. It delegates the message body to a pluggable
/// <see cref="ISerializer"/> and is the sole owner of the reserved <see cref="MetadataKey"/> envelope:
/// the body is placed at the top level and framework metadata (contract name/version, headers) is attached
/// under a single reserved key. The key is omitted when there is no metadata to carry, so a plain message is
/// plain JSON. A payload that carries no such key — e.g. an S3 event or any message from a producer outside
/// this library — is read as-is, so foreign messages can be consumed, and this library's messages can be
/// consumed by foreign readers, without ceremony. A custom <see cref="ISerializer"/> only controls the body;
/// it can never change or omit this envelope.
/// </summary>
internal sealed class EnvelopeSerializer(ISerializer body) : IMessageSerializer
{
    /// <summary>The reserved envelope key. Message types may not declare a property with this name.</summary>
    internal const string MetadataKey = "__runax";

    private readonly ISerializer _body = body;

    /// <inheritdoc />
    public string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers)
    {
        if (JsonNode.Parse(_body.Serialize(message)) is not JsonObject body)
        {
            throw new InvalidOperationException(
                $"Message of type '{typeof(TMessage).Name}' must serialize to a JSON object to carry the " +
                $"'{MetadataKey}' metadata. Arrays and primitives are not supported as top-level messages.");
        }

        if (body.ContainsKey(MetadataKey))
        {
            throw new InvalidOperationException(
                $"Message of type '{typeof(TMessage).Name}' declares a reserved '{MetadataKey}' property.");
        }

        var metadata = BuildMetadata(MessageContractCache.For(typeof(TMessage)), headers);
        if (metadata.Count > 0)
            body[MetadataKey] = metadata;

        return body.ToJsonString();
    }

    /// <inheritdoc />
    public MessageContext Deserialize(string payload, string topic)
    {
        var node = JsonNode.Parse(payload);

        string body;
        var metadata = Metadata.None;

        if (node is JsonObject obj && obj[MetadataKey] is JsonObject meta)
        {
            obj.Remove(MetadataKey);
            body = obj.ToJsonString();
            metadata = ReadMetadata(meta);
        }
        else
        {
            // Foreign / raw message: no reserved metadata, so the whole payload is the body.
            body = payload;
        }

        return new MessageContext
        {
            Topic = topic,
            Body = body,
            Headers = metadata.Headers,
            ContractName = metadata.ContractName,
            ContractVersion = metadata.ContractVersion,
            ContractSchema = metadata.ContractSchema,
            Serializer = _body,
        };
    }

    /// <summary>
    /// Reads the reserved metadata of a serialized envelope without materializing its body. A payload that is
    /// not JSON, or carries no reserved key, has none.
    /// </summary>
    internal static Metadata ReadMetadata(string payload)
    {
        try
        {
            return JsonNode.Parse(payload) is JsonObject obj && obj[MetadataKey] is JsonObject meta
                ? ReadMetadata(meta)
                : Metadata.None;
        }
        catch (JsonException)
        {
            return Metadata.None;
        }
    }

    /// <inheritdoc />
    public string EnrichHeaders(string payload, IReadOnlyDictionary<string, string> headers)
    {
        // Non-object payloads can't carry the reserved metadata; leave them untouched.
        if (JsonNode.Parse(payload) is not JsonObject obj)
            return payload;

        if (obj[MetadataKey] is not JsonObject meta)
        {
            meta = new JsonObject();
            obj[MetadataKey] = meta;
        }

        if (meta["headers"] is not JsonObject headerObject)
        {
            headerObject = new JsonObject();
            meta["headers"] = headerObject;
        }

        foreach (var (key, value) in headers)
            headerObject[key] = value;

        return obj.ToJsonString();
    }

    private static Metadata ReadMetadata(JsonObject meta)
    {
        var headers = new Dictionary<string, string>();
        if (meta["headers"] is JsonObject headerObject)
        {
            foreach (var (key, value) in headerObject)
                headers[key] = (string?)value ?? string.Empty;
        }

        // A non-string contract_schema is ignored rather than failing the whole envelope.
        var schema = meta["contract_schema"] is JsonValue schemaValue && schemaValue.TryGetValue<string>(out var text)
            ? ContractSchemaId.NormalizeOrNull(text)
            : null;

        return new Metadata((string?)meta["contract_name"], (int?)meta["contract_version"], headers)
        {
            ContractSchema = schema,
        };
    }

    private static JsonObject BuildMetadata(MessageContractAttribute? contract, IDictionary<string, string>? headers)
    {
        var meta = new JsonObject();

        if (contract is not null)
        {
            if (contract.Schema is { } schema)
                meta["contract_schema"] = ContractSchemaId.Normalize(schema);
            meta["contract_version"] = contract.Version;
            if (contract.Name is { } name)
                meta["contract_name"] = name;
        }

        if (headers is { Count: > 0 })
        {
            var headerObject = new JsonObject();
            foreach (var (key, value) in headers)
                headerObject[key] = value;
            meta["headers"] = headerObject;
        }

        return meta;
    }

    /// <summary>The framework metadata an envelope carries under its reserved key.</summary>
    internal sealed record Metadata(string? ContractName, int? ContractVersion, IReadOnlyDictionary<string, string> Headers)
    {
        public static Metadata None { get; } = new(null, null, ReadOnlyDictionary<string, string>.Empty);

        /// <summary>The contract schema <c>$id</c>, normalized; <see langword="null"/> when absent.</summary>
        public string? ContractSchema { get; init; }
    }
}
