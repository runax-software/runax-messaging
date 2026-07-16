using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.RabbitMq;

/// <summary>
/// Configurator extensions for the RabbitMQ transport.
/// </summary>
public static class RabbitMqConfiguratorExtensions
{
    /// <summary>
    /// Registers RabbitMQ as the messaging transport.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="RabbitMqOptions"/>.</param>
    public static MessagingConfigurator AddRabbitMq(
        this MessagingConfigurator configurator,
        Action<RabbitMqOptions> configure)
    {
        var options = new RabbitMqOptions();
        configure(options);

        configurator.Services.AddSingleton(options);
        configurator.Services.AddSingleton<IMessagingTransport, RabbitMqTransport>();

        return configurator;
    }
}
