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
    /// Gets the name of the bus this consumer subscribes on. The same consumer type may be
    /// registered on several buses (one registration per bus).
    /// </summary>
    public required string Bus { get; init; }
}
