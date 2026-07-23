using Microsoft.Extensions.DependencyInjection;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Scopes registrations (such as consumers) to a single transport. Passed to the configuration block of a
/// transport's <c>Add*</c> method so that anything registered through it binds to that broker.
/// </summary>
public sealed class TransportBuilder(IServiceCollection services, string transportName)
{
    /// <summary>
    /// Gets the underlying service collection.
    /// </summary>
    public IServiceCollection Services { get; } = services;

    /// <summary>
    /// Gets the <see cref="IMessagingTransport.SystemName"/> of the transport this builder scopes to.
    /// </summary>
    public string TransportName { get; } = transportName;
}
