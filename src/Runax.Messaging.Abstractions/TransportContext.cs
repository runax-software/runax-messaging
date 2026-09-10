namespace Runax.Messaging.Abstractions;

/// <summary>
/// Runtime context handed to <see cref="TransportConfig.CreateTransport"/>.
/// </summary>
public sealed class TransportContext
{
    /// <summary>
    /// Creates a context for the given bus.
    /// </summary>
    /// <param name="services">The application service provider.</param>
    /// <param name="busName">The name of the bus the transport is being created for.</param>
    /// <param name="mode">The bus's declared mode.</param>
    public TransportContext(IServiceProvider services, string busName, BusMode mode)
    {
        Services = services;
        BusName = busName;
        Mode = mode;
    }

    /// <summary>Gets the application service provider (for loggers, broker SDK clients, ...).</summary>
    public IServiceProvider Services { get; }

    /// <summary>Gets the name of the bus the transport belongs to.</summary>
    public string BusName { get; }

    /// <summary>
    /// Gets the bus's <see cref="BusMode"/>. Transports use it to skip building the unused
    /// side — no producer or publish pool on a <see cref="BusMode.ConsumeOnly"/> bus, no
    /// subscription resources on a <see cref="BusMode.PublishOnly"/> bus.
    /// </summary>
    public BusMode Mode { get; }
}
