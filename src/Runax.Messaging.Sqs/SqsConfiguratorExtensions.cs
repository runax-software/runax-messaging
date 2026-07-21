using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Sqs;

/// <summary>
/// Configurator extensions for the Amazon SQS transport.
/// </summary>
public static class SqsConfiguratorExtensions
{
    /// <summary>
    /// Registers Amazon SQS as the messaging transport.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="SqsOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddSqs(
        this MessagingConfigurator configurator,
        Action<SqsOptions> configure)
    {
        configurator.Services
            .AddOptions<SqsOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

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
