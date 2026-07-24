namespace Runax.Messaging.Abstractions;

/// <summary>
/// Serializes a message body to and from JSON. Register a custom implementation with
/// <c>UseSerializer&lt;T&gt;()</c> to control how bodies are encoded — for example a source-generated,
/// case-insensitive, or third-party JSON serializer.
/// </summary>
/// <remarks>
/// This is the <em>body</em> only. The framework always frames its reserved <c>__runax</c> metadata
/// envelope (contract name/version and headers) around whatever a serializer produces, so a custom
/// serializer can never change, move, or omit that envelope — it stays byte-for-byte the same regardless
/// of which serializer is active. <see cref="Serialize{TMessage}"/> must return a JSON <em>object</em> so
/// the metadata can be attached as a sibling key.
/// </remarks>
public interface ISerializer
{
    /// <summary>
    /// Serializes <paramref name="message"/> to a JSON string. The result must be a JSON object so the
    /// framework can attach its reserved <c>__runax</c> metadata as a sibling key.
    /// </summary>
    /// <typeparam name="TMessage">The message type to serialize.</typeparam>
    /// <param name="message">The message to serialize.</param>
    /// <returns>The message encoded as a JSON object string.</returns>
    string Serialize<TMessage>(TMessage message);

    /// <summary>
    /// Deserializes a JSON body — with the reserved <c>__runax</c> metadata already removed — into
    /// <typeparamref name="TMessage"/>.
    /// </summary>
    /// <typeparam name="TMessage">The type to deserialize into.</typeparam>
    /// <param name="body">The JSON body to deserialize.</param>
    /// <returns>The deserialized message, or <see langword="null"/> if the body is JSON null.</returns>
    TMessage? Deserialize<TMessage>(string body);
}
