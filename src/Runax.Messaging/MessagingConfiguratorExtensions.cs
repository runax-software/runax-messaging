using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;

namespace Runax.Messaging;

/// <summary>
/// Configurator extensions for registering consumers.
/// </summary>
public static class MessagingConfiguratorExtensions
{
    /// <summary>
    /// Registers a message consumer to be started by the hosted service.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to register.</typeparam>
    /// <param name="configurator">The messaging configurator.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddConsumer<TConsumer>(this MessagingConfigurator configurator)
        where TConsumer : class
    {
        configurator.Services.AddSingleton<TConsumer>();
        configurator.Services.AddSingleton(new ConsumerRegistration { ConsumerType = typeof(TConsumer) });
        return configurator;
    }

    /// <summary>
    /// Configures the retry and dead-letter policy applied to all consumers.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="RetryOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator WithRetry(
        this MessagingConfigurator configurator,
        Action<RetryOptions> configure)
    {
        configurator.Services
            .AddOptions<RetryOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return configurator;
    }

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> used to serialize and deserialize message bodies.
    /// Set a <see cref="System.Text.Json.Serialization.Metadata.IJsonTypeInfoResolver"/> here (e.g. a
    /// source-generated <c>JsonSerializerContext</c>) for a trim-friendly / AOT path.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure the shared <see cref="JsonSerializerOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator ConfigureSerialization(
        this MessagingConfigurator configurator,
        Action<JsonSerializerOptions> configure)
    {
        configurator.Services.Configure(configure);
        return configurator;
    }
}
