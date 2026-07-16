using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging;

/// <summary>
/// Configurator extensions for the in-memory transport.
/// </summary>
public static class InMemoryConfiguratorExtensions
{
    /// <summary>
    /// Registers the in-process transport. Messages are delivered within the same process.
    /// </summary>
    public static MessagingConfigurator AddInMemory(this MessagingConfigurator configurator)
    {
        configurator.Services.AddSingleton<IMessagingTransport, InMemoryTransport>();
        return configurator;
    }
}
