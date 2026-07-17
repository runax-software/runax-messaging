using System.Text.Json;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Provides context for a received message, including the raw body, topic, and headers.
/// </summary>
public sealed class MessageContext
{
    /// <summary>
    /// Gets the topic from which the message was received.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the raw JSON body of the message.
    /// </summary>
    public required string Body { get; init; }

    /// <summary>
    /// Gets the transport-level headers associated with the message.
    /// </summary>
    public required IReadOnlyDictionary<string, string> Headers { get; init; }

    /// <summary>
    /// Deserializes the message body to the specified type.
    /// </summary>
    /// <typeparam name="T">The type to deserialize the body into.</typeparam>
    /// <returns>The deserialized message, or <see langword="null"/> if the body is JSON null.</returns>
    public T? Deserialize<T>() => JsonSerializer.Deserialize<T>(Body);
}
