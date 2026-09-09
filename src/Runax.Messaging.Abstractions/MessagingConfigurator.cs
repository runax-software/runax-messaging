using Microsoft.Extensions.DependencyInjection;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Fluent surface for configuring messaging. Buses are attached through the <c>AddBus</c>
/// extension methods (in the <c>Runax.Messaging</c> package); each bus wraps exactly one
/// transport registered via <c>bus.AddTransport(...)</c>.
/// </summary>
public sealed class MessagingConfigurator
{
    /// <summary>
    /// Creates a configurator over the given service collection.
    /// </summary>
    /// <param name="services">The service collection that buses register into.</param>
    public MessagingConfigurator(IServiceCollection services) => Services = services;

    /// <summary>
    /// Gets the underlying service collection that buses register into.
    /// </summary>
    public IServiceCollection Services { get; }
}
