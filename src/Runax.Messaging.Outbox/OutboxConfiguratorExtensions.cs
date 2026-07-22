using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Outbox;

/// <summary>
/// Configurator extensions for the transactional outbox.
/// </summary>
public static class OutboxConfiguratorExtensions
{
    /// <summary>
    /// Routes <see cref="IMessagePublisher"/> through the outbox: publishes are serialized and written to
    /// the registered <see cref="IOutboxStore"/>, and a background dispatcher delivers them to the transport.
    /// Register a store with <see cref="AddInMemoryOutboxStore"/> or your own implementation.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Optional action to configure <see cref="OutboxOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddOutbox(
        this MessagingConfigurator configurator,
        Action<OutboxOptions>? configure = null)
    {
        var options = configurator.Services
            .AddOptions<OutboxOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();

        if (configure is not null) options.Configure(configure);

        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<OutboxOptions>>().Value);

        // Last IMessagePublisher registration wins, so publishes now flow into the outbox.
        configurator.Services.AddSingleton<IMessagePublisher, OutboxPublisher>();
        configurator.Services.AddHostedService<OutboxDispatcher>();

        return configurator;
    }

    /// <summary>
    /// Registers the in-process <see cref="InMemoryOutboxStore"/>. Intended for tests and single-process
    /// scenarios; it is not durable or transactional.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddInMemoryOutboxStore(this MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton<IOutboxStore, InMemoryOutboxStore>();
        return configurator;
    }
}
