using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub;

/// <summary>
/// Configurator extensions for the Google Cloud Pub/Sub transport.
/// </summary>
public static class GooglePubSubConfiguratorExtensions
{
    /// <summary>
    /// Registers Google Cloud Pub/Sub as the messaging transport.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="GooglePubSubOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddGooglePubSub(
        this MessagingConfigurator configurator,
        Action<GooglePubSubOptions> configure)
    {
        configurator.Services
            .AddOptions<GooglePubSubOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddGooglePubSubCore(configurator);
    }

    /// <summary>
    /// Registers Google Cloud Pub/Sub as the messaging transport, binding <see cref="GooglePubSubOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddGooglePubSub(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<GooglePubSubOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddGooglePubSubCore(configurator);
    }

    private static MessagingConfigurator AddGooglePubSubCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<GooglePubSubOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, GooglePubSubTransport>();

        return configurator;
    }
}
