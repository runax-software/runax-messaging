namespace Runax.Messaging.Abstractions;

/// <summary>
/// A message that arrived on a subscribed topic but that no registered consumer accepts — typically a
/// contract version this application does not (yet) handle. Passed to an <see cref="IUnroutableMessageHandler"/>
/// to decide its fate.
/// </summary>
public sealed class UnroutableMessage
{
    /// <summary>
    /// Gets the topic the message arrived on.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the contract name from the envelope, if any.
    /// </summary>
    public string? ContractName { get; init; }

    /// <summary>
    /// Gets the contract version from the envelope, or <see langword="null"/> if the message was unversioned.
    /// </summary>
    public int? ContractVersion { get; init; }

    /// <summary>
    /// Gets the raw JSON body of the message.
    /// </summary>
    public required string Body { get; init; }

    /// <summary>
    /// Gets the message headers.
    /// </summary>
    public required IReadOnlyDictionary<string, string> Headers { get; init; }

    /// <summary>
    /// Gets the <see cref="IMessagingTransport.SystemName"/> of the transport that delivered the message.
    /// </summary>
    public required string TransportSystemName { get; init; }
}
