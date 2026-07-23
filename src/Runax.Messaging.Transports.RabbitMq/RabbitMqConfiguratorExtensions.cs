using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.RabbitMq;

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
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddRabbitMq(
        this MessagingConfigurator configurator,
        Action<RabbitMqOptions> configure)
    {
        configurator.Services
            .AddOptions<RabbitMqOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddRabbitMqCore(configurator);
    }

    /// <summary>
    /// Registers RabbitMQ as the messaging transport, binding <see cref="RabbitMqOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddRabbitMq(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<RabbitMqOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddRabbitMqCore(configurator);
    }

    /// <summary>
    /// Registers RabbitMQ as the messaging transport and scopes consumers to it via the builder block.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="RabbitMqOptions"/>.</param>
    /// <param name="configureTransport">Block that registers consumers bound to this broker.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddRabbitMq(
        this MessagingConfigurator configurator,
        Action<RabbitMqOptions> configure,
        Action<TransportBuilder> configureTransport)
    {
        AddRabbitMq(configurator, configure);
        configureTransport(new TransportBuilder(configurator.Services, RabbitMqTransport.TransportName));
        return configurator;
    }

    private static MessagingConfigurator AddRabbitMqCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<RabbitMqOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, RabbitMqTransport>();

        return configurator;
    }
}
