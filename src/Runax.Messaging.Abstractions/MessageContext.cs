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
    /// Gets the contract name carried in the envelope, or <see langword="null"/> for an unversioned
    /// message or one whose contract left the name unset (the topic is then the effective identity).
    /// </summary>
    public string? ContractName { get; init; }

    /// <summary>
    /// Gets the contract version carried in the envelope, or <see langword="null"/> for an unversioned message.
    /// </summary>
    public int? ContractVersion { get; init; }

    /// <summary>
    /// Gets the <c>$id</c> of the contract schema the message carried (in the envelope or as a native
    /// header), or <see langword="null"/> when it carried none.
    /// </summary>
    public string? ContractSchema { get; init; }

    /// <summary>
    /// Gets the contract schema <c>$id</c>s the body validated against when the message carried no
    /// identity and was routed by validation. Empty otherwise. Unlike <see cref="ContractSchema"/>,
    /// these were inferred, not claimed by the producer.
    /// </summary>
    public IReadOnlyList<string> InferredContractSchemas { get; init; } = [];

    /// <summary>
    /// Gets the body serializer threaded through from the framework so <see cref="Deserialize{T}"/> uses the
    /// same encoding the message was written with. When <see langword="null"/> (e.g. a hand-built context),
    /// default System.Text.Json is used.
    /// </summary>
    internal ISerializer? Serializer { get; init; }

    /// <summary>
    /// Deserializes the message body to the specified type using the configured serializer.
    /// </summary>
    /// <typeparam name="T">The type to deserialize the body into.</typeparam>
    /// <returns>The deserialized message, or <see langword="null"/> if the body is JSON null.</returns>
    public T? Deserialize<T>() => (Serializer ?? SystemTextJsonSerializer.Default).Deserialize<T>(Body);
}
