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
}
