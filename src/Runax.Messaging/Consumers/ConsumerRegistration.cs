namespace Runax.Messaging.Consumers;

/// <summary>
/// Tracks metadata about a registered consumer for the hosting infrastructure.
/// </summary>
internal sealed class ConsumerRegistration
{
    /// <summary>
    /// Gets the CLR type of the consumer class.
    /// </summary>
    public required Type ConsumerType { get; init; }

    /// <summary>
    /// Gets the system names of the transports this consumer subscribes to, or <c>null</c>
    /// to subscribe on every registered transport. Names are matched against
    /// <see cref="Abstractions.IMessagingTransport.SystemName"/>.
    /// </summary>
    public IReadOnlyList<string>? Transports { get; init; }
}
