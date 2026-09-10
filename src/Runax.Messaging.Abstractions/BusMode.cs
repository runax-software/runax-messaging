namespace Runax.Messaging.Abstractions;

/// <summary>
/// Declares an application's relationship with a bus's broker. Set via
/// <c>bus.Mode = BusMode.ConsumeOnly</c> inside the <c>AddBus</c> block; violations are
/// validated when the block completes, so a misconfigured bus fails at startup.
/// </summary>
public enum BusMode
{
    /// <summary>The default: the bus both publishes and consumes.</summary>
    PublishAndConsume,

    /// <summary>
    /// The application only publishes on this bus. Registering a consumer (or a
    /// consume-side policy such as a retry or unroutable-message handler) throws at
    /// configuration time, and no consumer hosted service is registered for the bus.
    /// </summary>
    PublishOnly,

    /// <summary>
    /// The application only consumes on this bus. Publishing via <see cref="IBus"/> throws,
    /// and configuring an outbox throws at configuration time. Framework-internal publishes
    /// performed by the consume pipeline (dead-letter enrichment) remain allowed; when broker
    /// credentials cannot write at all, use <c>DeadLetterStrategy.BrokerNative</c> or disable
    /// dead-lettering on this bus.
    /// </summary>
    ConsumeOnly,
}
