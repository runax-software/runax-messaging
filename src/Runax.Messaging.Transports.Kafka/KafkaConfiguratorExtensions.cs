using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka;

/// <summary>
/// Configurator extensions for the Apache Kafka transport.
/// </summary>
public static class KafkaConfiguratorExtensions
{
    /// <summary>
    /// Registers Apache Kafka as the messaging transport, configuring options and consumers in one block:
    /// <c>AddKafka(kafka =&gt; { kafka.Configure(o =&gt; ...); kafka.AddConsumer&lt;T&gt;(); })</c>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that configures <see cref="KafkaOptions"/> (via <c>Configure</c>) and registers consumers.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddKafka(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<KafkaOptions>> configure)
    {
        var builder = new TransportBuilder<KafkaOptions>(configurator.Services, KafkaTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<KafkaOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

        return AddKafkaCore(configurator);
    }

    /// <summary>
    /// Registers Apache Kafka as the messaging transport, binding <see cref="KafkaOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddKafka(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<KafkaOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddKafkaCore(configurator);
    }

    private static MessagingConfigurator AddKafkaCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<KafkaOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, KafkaTransport>();

        return configurator;
    }
}
