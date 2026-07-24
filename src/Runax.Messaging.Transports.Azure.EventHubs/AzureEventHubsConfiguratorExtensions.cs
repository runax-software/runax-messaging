using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs;

/// <summary>
/// Configurator extensions for the Azure Event Hubs transport.
/// </summary>
public static class AzureEventHubsConfiguratorExtensions
{
    /// <summary>
    /// Registers Azure Event Hubs as the messaging transport, configuring options and consumers in one block:
    /// <c>AddAzureEventHubs(eh =&gt; { eh.Configure(o =&gt; ...); eh.AddConsumer&lt;T&gt;(); })</c>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that configures <see cref="AzureEventHubsOptions"/> (via <c>Configure</c>) and registers consumers.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddAzureEventHubs(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<AzureEventHubsOptions>> configure)
    {
        var builder = new TransportBuilder<AzureEventHubsOptions>(configurator.Services, AzureEventHubsTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<AzureEventHubsOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

        return AddAzureEventHubsCore(configurator);
    }

    /// <summary>
    /// Registers Azure Event Hubs as the messaging transport, binding <see cref="AzureEventHubsOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddAzureEventHubs(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<AzureEventHubsOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddAzureEventHubsCore(configurator);
    }

    private static MessagingConfigurator AddAzureEventHubsCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<AzureEventHubsOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, AzureEventHubsTransport>();

        return configurator;
    }
}
