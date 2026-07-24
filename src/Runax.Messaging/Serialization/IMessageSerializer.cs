using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Framework-owned wire contract: it frames the reserved <c>__runax</c> envelope (the payload at the top
/// level, framework metadata under the reserved key) around a body produced by a pluggable
/// <see cref="ISerializer"/>. This is not the customization point — to change how bodies are encoded,
/// implement <see cref="ISerializer"/> and register it with <c>UseSerializer&lt;T&gt;()</c>; the envelope
/// then stays identical regardless. <see cref="Deserialize"/> returns a body as a JSON string that
/// <see cref="MessageContext.Deserialize{T}"/> reads.
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
