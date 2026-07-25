using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Extension methods for registering Runax.Messaging services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Runax messaging module and applies the given configuration.
    /// A transport (e.g. <c>AddInMemory</c>, <c>AddSqs</c>) must be configured for publishing or consuming to work.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Action to configure the transport and consumers.</param>
    /// <returns>The same service collection, to allow chaining.</returns>
    public static IServiceCollection AddRunaxMessaging(
        this IServiceCollection services,
        Action<MessagingConfigurator> configure)
    {
        services.AddOptions();

        // Resolve the configured JsonSerializerOptions (default when ConfigureSerialization was not called).
        services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<JsonSerializerOptions>>().Value);
        // Default body serializer; UseSerializer<T> overrides ISerializer without touching the envelope.
        services.TryAddSingleton<ISerializer, SystemTextJsonSerializer>();
        services.TryAddSingleton<IMessageSerializer, EnvelopeSerializer>();
        // Resolves the serializer per transport, honoring per-broker UseSerializer/ConfigureSerialization.
        services.TryAddSingleton<IMessageSerializerProvider, MessageSerializerProvider>();
        services.TryAddSingleton<MessagingPublishOptions>();
        // Hands out publishers pinned to a named transport for explicit per-transport publishing.
        services.TryAddSingleton<IMessagePublisherFactory, MessagePublisherFactory>();
        // The default publisher targets the sole transport, or the one chosen with PublishTo.
        services.TryAddSingleton<IMessagePublisher, DefaultMessagePublisher>();
        services.TryAddSingleton<IUnroutableMessageHandler, DeadLetterUnroutableHandler>();
        // Resolves the unroutable handler per transport, honoring per-broker OnUnroutableMessage.
        services.TryAddSingleton<IUnroutableMessageHandlerProvider, UnroutableMessageHandlerProvider>();
        // Resolves the retry policy per transport, honoring per-broker WithRetry.
        services.TryAddSingleton<IRetryOptionsProvider, RetryOptionsProvider>();
        services.TryAddSingleton<IMessageContractCatalog, MessageContractCatalog>();

        configure(new MessagingConfigurator(services));

        // Falls back to defaults (via IOptions) when the caller did not configure a policy via WithRetry.
        services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<RetryOptions>>().Value);

        if (services.Any(descriptor => descriptor.ServiceType == typeof(ConsumerRegistration)))
            services.AddHostedService<MessageConsumerHostedService>();

        return services;
    }
}
