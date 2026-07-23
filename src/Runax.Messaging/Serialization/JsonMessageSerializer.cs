using System.Text.Json;
using System.Text.Json.Nodes;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Default serializer. The message is serialized at the top level and framework metadata (contract, version,
/// headers) is attached under a single reserved <see cref="MetadataKey"/> object. A payload that carries no
/// such key — e.g. an S3 event or any message from a producer outside this library — is read as-is, so foreign
/// messages can be consumed, and this library's messages can be consumed by foreign readers, without ceremony.
/// </summary>
internal sealed class JsonMessageSerializer(JsonSerializerOptions? bodyOptions = null) : IMessageSerializer
{
    /// <summary>The reserved envelope key. Message types may not declare a property with this name.</summary>
    internal const string MetadataKey = "__runax";

    private readonly JsonSerializerOptions _bodyOptions = bodyOptions ?? new JsonSerializerOptions();

    /// <inheritdoc />
    public string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers)
    {
        if (JsonSerializer.SerializeToNode(message, _bodyOptions) is not JsonObject body)
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

        body[MetadataKey] = BuildMetadata(MessageContractCache.For(typeof(TMessage)), headers);
        return body.ToJsonString();
    }

    /// <inheritdoc />
    public MessageContext Deserialize(string payload, string topic)
    {
        var node = JsonNode.Parse(payload);

        string body;
        string? contractName = null;
        int? contractVersion = null;
        var headers = new Dictionary<string, string>();

        if (node is JsonObject obj && obj[MetadataKey] is JsonObject meta)
        {
            obj.Remove(MetadataKey);
            body = obj.ToJsonString();
            contractName = (string?)meta["contract_name"];
            contractVersion = (int?)meta["contract_version"];
            if (meta["headers"] is JsonObject headerObject)
            {
                foreach (var (key, value) in headerObject)
                    headers[key] = (string?)value ?? string.Empty;
            }
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
            Headers = headers,
            ContractName = contractName,
            ContractVersion = contractVersion,
            SerializerOptions = _bodyOptions,
        };
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

    private static JsonObject BuildMetadata(MessageContractAttribute? contract, IDictionary<string, string>? headers)
    {
        var meta = new JsonObject();

        if (contract is not null)
        {
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
}
