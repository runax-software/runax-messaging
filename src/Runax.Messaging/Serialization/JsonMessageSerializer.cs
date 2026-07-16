using System.Text.Json;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// System.Text.Json-based message serializer.
/// </summary>
internal sealed class JsonMessageSerializer : IMessageSerializer
{
    /// <inheritdoc />
    public string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers)
    {
        return JsonSerializer.Serialize(new MessageEnvelope
        {
            MessageType = typeof(TMessage).AssemblyQualifiedName,
            Body = JsonSerializer.Serialize(message),
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
        };
    }
}
