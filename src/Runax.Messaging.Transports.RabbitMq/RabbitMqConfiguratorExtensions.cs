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
    /// Registers RabbitMQ as the messaging transport, configuring options and consumers in one block:
    /// <c>AddRabbitMq(rabbit =&gt; { rabbit.Configure(o =&gt; ...); rabbit.AddConsumer&lt;T&gt;(); })</c>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that configures <see cref="RabbitMqOptions"/> (via <c>Configure</c>) and registers consumers.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddRabbitMq(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<RabbitMqOptions>> configure)
    {
        var builder = new TransportBuilder<RabbitMqOptions>(configurator.Services, RabbitMqTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<RabbitMqOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

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

    private static MessagingConfigurator AddRabbitMqCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<RabbitMqOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, RabbitMqTransport>();

        return configurator;
    }
}
