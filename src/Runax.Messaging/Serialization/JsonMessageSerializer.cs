using System.Text.Json;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// System.Text.Json-based message serializer. Message bodies use the configured
/// <see cref="JsonSerializerOptions"/>; the envelope wrapper always uses defaults so the wire
/// format stays stable regardless of body configuration.
/// </summary>
internal sealed class JsonMessageSerializer(JsonSerializerOptions? bodyOptions = null) : IMessageSerializer
{
    private readonly JsonSerializerOptions _bodyOptions = bodyOptions ?? new JsonSerializerOptions();

    /// <inheritdoc />
    public string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers)
    {
        return JsonSerializer.Serialize(new MessageEnvelope
        {
            MessageType = typeof(TMessage).AssemblyQualifiedName,
            Body = JsonSerializer.Serialize(message, _bodyOptions),
            Headers = headers is not null
                ? new Dictionary<string, string>(headers)
                : new Dictionary<string, string>(),
        });
    }

    /// <inheritdoc />
    public MessageContext Deserialize(string envelopeJson, string topic)
    {
        var envelope = JsonSerializer.Deserialize<MessageEnvelope>(envelopeJson)
                       ?? throw new InvalidOperationException("Failed to deserialize message envelope.");

        return new MessageContext
        {
            Topic = topic,
            Body = envelope.Body ?? string.Empty,
            Headers = envelope.Headers is not null
                ? new Dictionary<string, string>(envelope.Headers)
                : new Dictionary<string, string>(),
            SerializerOptions = _bodyOptions,
        };
    }

    /// <inheritdoc />
    public string EnrichHeaders(string envelopeJson, IReadOnlyDictionary<string, string> headers)
    {
        var envelope = JsonSerializer.Deserialize<MessageEnvelope>(envelopeJson)
                       ?? throw new InvalidOperationException("Failed to deserialize message envelope.");

        var merged = envelope.Headers is not null
            ? new Dictionary<string, string>(envelope.Headers)
            : new Dictionary<string, string>();

        foreach (var (key, value) in headers)
            merged[key] = value;

        return JsonSerializer.Serialize(new MessageEnvelope
        {
            MessageType = envelope.MessageType,
            Body = envelope.Body,
            Headers = merged,
        });
    }
}
