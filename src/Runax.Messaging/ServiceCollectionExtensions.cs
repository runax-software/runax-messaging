using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<IMessagePublisher, MessagePublisherAdapter>();

        configure(new MessagingConfigurator(services));

        // Falls back to defaults when the caller did not configure a policy via WithRetry.
        services.TryAddSingleton(new RetryOptions());

        if (services.Any(descriptor => descriptor.ServiceType == typeof(ConsumerRegistration)))
            services.AddHostedService<MessageConsumerHostedService>();

        return services;
    }
}
