using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;

namespace Runax.Messaging;

/// <summary>
/// Configurator extensions for registering consumers.
/// </summary>
public static class MessagingConfiguratorExtensions
{
    /// <summary>
    /// Registers a message consumer to be started by the hosted service.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to register.</typeparam>
    /// <param name="configurator">The messaging configurator.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddConsumer<TConsumer>(this MessagingConfigurator configurator)
        where TConsumer : class
    {
        configurator.Services.AddSingleton<TConsumer>();
        configurator.Services.AddSingleton(new ConsumerRegistration { ConsumerType = typeof(TConsumer) });
        return configurator;
    }
}
