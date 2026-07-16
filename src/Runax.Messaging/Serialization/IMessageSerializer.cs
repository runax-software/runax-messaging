using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Handles serialization and deserialization of message envelopes.
/// </summary>
internal interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message and optional headers into an envelope JSON string.
    /// </summary>
    string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers);

    /// <summary>
    /// Deserializes an envelope JSON string into a <see cref="MessageContext"/>.
    /// </summary>
    MessageContext Deserialize(string envelopeJson, string topic);
}
