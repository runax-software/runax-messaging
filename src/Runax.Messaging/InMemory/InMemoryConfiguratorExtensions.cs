using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.InMemory;

/// <summary>
/// Configurator extensions for the in-memory transport.
/// </summary>
public static class InMemoryConfiguratorExtensions
{
    /// <summary>
    /// Registers the in-process transport. Messages are delivered within the same process.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddInMemory(this MessagingConfigurator configurator)
    {
        configurator.Services.AddSingleton<IMessagingTransport, InMemoryTransport>();
        return configurator;
    }
}
