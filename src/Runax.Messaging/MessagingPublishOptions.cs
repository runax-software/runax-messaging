namespace Runax.Messaging;

/// <summary>
/// Controls how <see cref="Abstractions.IMessagePublisher"/> selects a transport when several are registered.
/// </summary>
internal sealed class MessagingPublishOptions
{
    /// <summary>
    /// Gets the system name of the transport publishes are routed to, or <c>null</c> to use the single
    /// registered transport. Set via <c>PublishTo</c>.
    /// </summary>
    public string? DefaultTransport { get; init; }
}
