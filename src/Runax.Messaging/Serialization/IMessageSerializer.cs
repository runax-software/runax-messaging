using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Encodes a message to the wire and decodes a received payload. Implement this and register it with
/// <c>UseSerializer&lt;T&gt;()</c> to control the wire format — for example to interoperate with messages
/// produced outside this library. The default implementation puts the payload at the top level with framework
/// metadata under a reserved <c>__runax</c> key. Serializers here are JSON-oriented: <see cref="Deserialize"/>
/// returns a body as a JSON string that <see cref="MessageContext.Deserialize{T}"/> reads.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Encodes a message and optional headers into the wire payload.
    /// </summary>
    string Serialize<TMessage>(TMessage message, IDictionary<string, string>? headers);

    /// <summary>
    /// Decodes a received wire payload into a <see cref="MessageContext"/> (body, headers, and — when present —
    /// the contract name and version).
    /// </summary>
    MessageContext Deserialize(string payload, string topic);

    /// <summary>
    /// Returns <paramref name="payload"/> with <paramref name="headers"/> merged into its header set (used when
    /// dead-lettering). Existing keys are overwritten; the body is preserved. Return the payload unchanged if the
    /// format cannot carry headers.
    /// </summary>
    string EnrichHeaders(string payload, IReadOnlyDictionary<string, string> headers);
}
