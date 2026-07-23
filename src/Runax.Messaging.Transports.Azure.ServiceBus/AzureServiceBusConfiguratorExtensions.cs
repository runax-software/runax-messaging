using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus;

/// <summary>
/// Configurator extensions for the Azure Service Bus transport.
/// </summary>
public static class AzureServiceBusConfiguratorExtensions
{
    /// <summary>
    /// Registers Azure Service Bus as the messaging transport.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="AzureServiceBusOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddAzureServiceBus(
        this MessagingConfigurator configurator,
        Action<AzureServiceBusOptions> configure)
    {
        configurator.Services
            .AddOptions<AzureServiceBusOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddAzureServiceBusCore(configurator);
    }

    /// <summary>
    /// Registers Azure Service Bus as the messaging transport, binding <see cref="AzureServiceBusOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddAzureServiceBus(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<AzureServiceBusOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddAzureServiceBusCore(configurator);
    }

    /// <summary>
    /// Registers Azure Service Bus as the messaging transport and scopes consumers to it via the builder block.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="AzureServiceBusOptions"/>.</param>
    /// <param name="configureTransport">Block that registers consumers bound to this broker.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddAzureServiceBus(
        this MessagingConfigurator configurator,
        Action<AzureServiceBusOptions> configure,
        Action<TransportBuilder> configureTransport)
    {
        AddAzureServiceBus(configurator, configure);
        configureTransport(new TransportBuilder(configurator.Services, AzureServiceBusTransport.TransportName));
        return configurator;
    }

    private static MessagingConfigurator AddAzureServiceBusCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<AzureServiceBusOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, AzureServiceBusTransport>();

        return configurator;
    }
}
