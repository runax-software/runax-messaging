using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sns;

/// <summary>
/// Configurator extensions for the Amazon SNS transport.
/// </summary>
public static class SnsConfiguratorExtensions
{
    /// <summary>
    /// Registers Amazon SNS as the messaging transport (publish to SNS, consume from a subscribed SQS queue).
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="SnsOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddSns(
        this MessagingConfigurator configurator,
        Action<SnsOptions> configure)
    {
        configurator.Services
            .AddOptions<SnsOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddSnsCore(configurator);
    }

    /// <summary>
    /// Registers Amazon SNS as the messaging transport, binding <see cref="SnsOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddSns(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<SnsOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddSnsCore(configurator);
    }

    /// <summary>
    /// Registers Amazon SNS as the messaging transport and scopes consumers to it via the builder block.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="SnsOptions"/>.</param>
    /// <param name="configureTransport">Block that registers consumers bound to this broker.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddSns(
        this MessagingConfigurator configurator,
        Action<SnsOptions> configure,
        Action<TransportBuilder> configureTransport)
    {
        AddSns(configurator, configure);
        configureTransport(new TransportBuilder(configurator.Services, SnsTransport.TransportName));
        return configurator;
    }

    private static MessagingConfigurator AddSnsCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<SnsOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, SnsTransport>();

        return configurator;
    }
}
