namespace Runax.Messaging.Serialization;

/// <summary>
/// Internal envelope used for serializing messages on the wire.
/// </summary>
internal sealed class MessageEnvelope
{
    /// <summary>
    /// Gets or sets the fully-qualified CLR type name of the message payload.
    /// </summary>
    public string? MessageType { get; init; }

    /// <summary>
    /// Gets or sets the JSON-serialized message body.
    /// </summary>
    public string? Body { get; init; }

    /// <summary>
    /// Gets or sets the message headers.
    /// </summary>
    public Dictionary<string, string>? Headers { get; init; }
}
