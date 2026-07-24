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
    /// Registers Amazon SNS as the messaging transport (publish to SNS, consume from a subscribed SQS queue),
    /// configuring options and consumers in one block:
    /// <c>AddSns(sns =&gt; { sns.Configure(o =&gt; ...); sns.AddConsumer&lt;T&gt;(); })</c>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that configures <see cref="SnsOptions"/> (via <c>Configure</c>) and registers consumers.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddSns(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<SnsOptions>> configure)
    {
        var builder = new TransportBuilder<SnsOptions>(configurator.Services, SnsTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<SnsOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

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

    private static MessagingConfigurator AddSnsCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<SnsOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, SnsTransport>();

        return configurator;
    }
}
