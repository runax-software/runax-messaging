using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sqs;

/// <summary>
/// Configurator extensions for the Amazon SQS transport.
/// </summary>
public static class SqsConfiguratorExtensions
{
    /// <summary>
    /// Registers Amazon SQS as the messaging transport, configuring options and consumers in one block:
    /// <c>AddSqs(sqs =&gt; { sqs.Configure(o =&gt; ...); sqs.AddConsumer&lt;T&gt;(); })</c>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that configures <see cref="SqsOptions"/> (via <c>Configure</c>) and registers consumers.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddSqs(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<SqsOptions>> configure)
    {
        var builder = new TransportBuilder<SqsOptions>(configurator.Services, SqsTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<SqsOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

        return AddSqsCore(configurator);
    }

    /// <summary>
    /// Registers Amazon SQS as the messaging transport, binding <see cref="SqsOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddSqs(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<SqsOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddSqsCore(configurator);
    }

    private static MessagingConfigurator AddSqsCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<SqsOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, SqsTransport>();

        return configurator;
    }
}
