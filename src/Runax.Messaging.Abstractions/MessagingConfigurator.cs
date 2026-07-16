using Microsoft.Extensions.DependencyInjection;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Fluent surface for configuring messaging. Transports and consumers are attached
/// through extension methods (e.g. <c>AddInMemory</c>, <c>AddSqs</c>, <c>AddConsumer</c>).
/// </summary>
public sealed class MessagingConfigurator
{
    /// <summary>
    /// Creates a configurator over the given service collection.
    /// </summary>
    public MessagingConfigurator(IServiceCollection services) => Services = services;

    /// <summary>
    /// Gets the underlying service collection that transports and consumers register into.
    /// </summary>
    public IServiceCollection Services { get; }
}
