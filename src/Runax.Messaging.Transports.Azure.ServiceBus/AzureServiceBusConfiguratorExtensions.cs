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
    /// Registers Azure Service Bus as the messaging transport, configuring options and consumers in one block:
    /// <c>AddAzureServiceBus(sb =&gt; { sb.Configure(o =&gt; ...); sb.AddConsumer&lt;T&gt;(); })</c>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that configures <see cref="AzureServiceBusOptions"/> (via <c>Configure</c>) and registers consumers.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddAzureServiceBus(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<AzureServiceBusOptions>> configure)
    {
        var builder = new TransportBuilder<AzureServiceBusOptions>(configurator.Services, AzureServiceBusTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<AzureServiceBusOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

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

    private static MessagingConfigurator AddAzureServiceBusCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<AzureServiceBusOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, AzureServiceBusTransport>();

        return configurator;
    }
}
