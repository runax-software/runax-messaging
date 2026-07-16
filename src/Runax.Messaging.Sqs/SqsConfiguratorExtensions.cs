using Microsoft.Extensions.DependencyInjection;
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
    public static MessagingConfigurator AddSqs(
        this MessagingConfigurator configurator,
        Action<SqsOptions> configure)
    {
        var options = new SqsOptions();
        configure(options);

        configurator.Services.AddSingleton(options);
        configurator.Services.AddSingleton<IMessagingTransport, SqsTransport>();

        return configurator;
    }
}
