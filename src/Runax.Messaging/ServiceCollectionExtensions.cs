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
    /// Registers the Runax messaging module and applies the given configuration. Register at least
    /// one bus (e.g. <c>messaging.AddBus(bus =&gt; bus.AddTransport(new InMemoryConfig()))</c>) for
    /// publishing or consuming to work.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Action that registers buses.</param>
    /// <returns>The same service collection, to allow chaining.</returns>
    public static IServiceCollection AddRunaxMessaging(
        this IServiceCollection services,
        Action<MessagingConfigurator> configure)
    {
        services.AddOptions();

        // Resolve the configured JsonSerializerOptions (default when never configured).
        services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<JsonSerializerOptions>>().Value);
        // Default body serializer; bus.UseSerializer<T> overrides per bus without touching the envelope.
        services.TryAddSingleton<ISerializer, SystemTextJsonSerializer>();
        services.TryAddSingleton<IMessageSerializer, EnvelopeSerializer>();
        // Resolves the serializer per bus/topic, honoring bus.UseSerializer / bus.ConfigureSerialization.
        services.TryAddSingleton<IMessageSerializerProvider, MessageSerializerProvider>();
        services.TryAddSingleton<IUnroutableMessageHandler, DeadLetterUnroutableHandler>();
        // Resolves the unroutable handler per bus, honoring bus.OnUnroutableMessage.
        services.TryAddSingleton<IUnroutableMessageHandlerProvider, UnroutableMessageHandlerProvider>();
        // Resolves the retry policy per bus/topic, honoring bus.WithRetry / bus.WithRetryForTopic.
        services.TryAddSingleton<IRetryOptionsProvider, RetryOptionsProvider>();
        services.TryAddSingleton<IMessageContractCatalog, MessageContractCatalog>();

        services.TryAddSingleton<IBusProvider, BusProvider>();

        // The unkeyed IBus: the default bus when one exists, else the sole named bus; ambiguous
        // configurations must inject a keyed IBus (or use IBusProvider).
        services.TryAddSingleton<IBus>(ResolveUnkeyedBus);

        configure(new MessagingConfigurator(services));

        // Built-in defaults for buses without a WithRetry policy.
        services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<RetryOptions>>().Value);

        return services;
    }

    private static IBus ResolveUnkeyedBus(IServiceProvider sp)
    {
        var registrations = sp.GetServices<BusRegistration>().ToArray();

        if (registrations.Any(r => r.Name == BusNames.Default))
            return sp.GetRequiredKeyedService<IBus>(BusNames.Default);

        return registrations.Length switch
        {
            0 => throw new InvalidOperationException(
                "No bus is registered. Add one inside AddRunaxMessaging " +
                "(e.g. messaging.AddBus(bus => bus.AddTransport(new InMemoryConfig()))) before resolving IBus."),
            1 => sp.GetRequiredKeyedService<IBus>(registrations[0].Name),
            _ => throw new InvalidOperationException(
                "Several named buses are registered and none is the default, so an unkeyed IBus is ambiguous. " +
                "Inject a keyed bus ([FromKeyedServices(\"<name>\")] IBus) or use IBusProvider.GetBus(...). " +
                $"Registered buses: {string.Join(", ", registrations.Select(r => r.Name))}."),
        };
    }
}
